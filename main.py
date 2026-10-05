import os, math, time, hmac, requests, threading
import psycopg
from datetime import datetime, timezone
from typing import Literal, Optional
from fastapi import FastAPI, HTTPException, Header, Request, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from monitoring import Monitor, Vitals
monitor = Monitor()
vitals = Vitals()

app=FastAPI(title="Rick's Trackers")
TOKEN=os.getenv("PARTICLE_WEBHOOK_TOKEN","")
TELEGRAM_BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN","")
TELEGRAM_CHAT_ID=os.getenv("TELEGRAM_CHAT_ID","")
PARTICLE_ACCESS_TOKEN=os.getenv("PARTICLE_ACCESS_TOKEN","")
PARTICLE_DEVICE_ID=os.getenv("PARTICLE_DEVICE_ID","")
HISTORY_DATABASE_URL=os.getenv("HISTORY_DATABASE_URL") or os.getenv("DATABASE_URL") or ""
state={"particle_control_configured":bool(PARTICLE_ACCESS_TOKEN and PARTICLE_DEVICE_ID),"mode":"off","geofence_ft":1000,"live":False,"alarm":False,"alarm_reason":None,"lat":None,"lon":None,"home_lat":None,"home_lon":None,"speed_mph":0,"battery_percent":None,"external_power":None,"lte":None,"lte_quality":None,"network_type":None,"gps_fix":None,"imu_sensitivity":None,"imu_pending":None,"tracker_sleep":None,"tracker_update_interval_sec":None,"last_seen":None,"vitals_updated_at":None,"route":[],"events":[],"timeline":[],"telegram_configured":False,"last_alert_at":None,"telegram_chat_id":TELEGRAM_CHAT_ID or None,"_key":None,"_epoch":0,"_motion_started":None,"_vitals_checked":0,"_last_motion_device_time":None,"_last_motion_published_at":None,"_last_motion_webhook_at":None,"_alarm_poll_next_at":None,"_armed_motion_sensitivity":"high"}

state.update({"tasking_state":"ready","readiness":"checking","readiness_message":"Checking tracker readiness","device_online":None,"mode_status":"unknown","mode_message":"Checking tracker settings", "_control_checked":0})
_mode_change_lock=threading.Lock()

_alarm_poll_thread=None
_alarm_poll_thread_lock=threading.Lock()
_alarm_poll_wake=threading.Event()
_alarm_poll_stop=threading.Event()

class Tasking(BaseModel): action:Literal["startup","storage"]
class Mode(BaseModel): mode:Literal["armed","geofence","off"]
class Fence(BaseModel): feet:Literal[100,500,1000]
class Live(BaseModel): enabled:bool
class ImuSensitivity(BaseModel): sensitivity:Literal["low","medium","high"]
class Event(BaseModel):
    lat:float; lon:float; speed_mph:float=0; battery_percent:Optional[int]=None
    external_power:Optional[bool]=None; lte:Optional[str]=None; gps_fix:bool=True
    motion:bool=False; alarm:bool=False; alarm_reason:Optional[str]=None; timestamp:Optional[str]=None

def pub(): return {k:v for k,v in state.items() if not k.startswith("_")}

_history_db_ready=False
_history_db_error=None

def history_init():
    global _history_db_ready,_history_db_error
    if not HISTORY_DATABASE_URL:
        _history_db_ready=False
        _history_db_error="History database URL is not configured"
        return False
    try:
        with psycopg.connect(HISTORY_DATABASE_URL,connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tracker_history (
                        id BIGSERIAL PRIMARY KEY,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        event_time TIMESTAMPTZ,
                        category TEXT NOT NULL,
                        title TEXT NOT NULL,
                        detail TEXT,
                        mode TEXT,
                        alarm BOOLEAN,
                        lat DOUBLE PRECISION,
                        lon DOUBLE PRECISION,
                        speed_mph DOUBLE PRECISION,
                        battery_percent INTEGER,
                        external_power BOOLEAN,
                        lte TEXT,
                        gps_fix BOOLEAN
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS tracker_history_created_at_idx ON tracker_history(created_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS tracker_history_category_idx ON tracker_history(category)")
                cur.execute("CREATE TABLE IF NOT EXISTS tracker_settings (id INTEGER PRIMARY KEY CHECK (id=1), mode TEXT NOT NULL CHECK (mode IN ('off','armed','geofence')))")
                cur.execute("ALTER TABLE tracker_settings ADD COLUMN IF NOT EXISTS tasking_state TEXT NOT NULL DEFAULT 'ready'")
        _history_db_ready=True
        _history_db_error=None
        return True
    except Exception as e:
        _history_db_ready=False
        _history_db_error=str(e)
        print(f"History database init failed: {e}")
        return False

def history_record(category,title,detail="",event_time=None,lat=None,lon=None,
                   speed_mph=None,battery_percent=None,external_power=None,lte=None,gps_fix=None):
    global _history_db_ready,_history_db_error
    if not HISTORY_DATABASE_URL:
        return False
    try:
        if not _history_db_ready and not history_init():
            return False
        with psycopg.connect(HISTORY_DATABASE_URL,connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO tracker_history
                    (event_time,category,title,detail,mode,alarm,lat,lon,speed_mph,
                     battery_percent,external_power,lte,gps_fix)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,(
                    event_time,category,title,detail,state.get("mode"),bool(state.get("alarm")),
                    lat,lon,speed_mph,battery_percent,external_power,lte,gps_fix
                ))
        return True
    except Exception as e:
        _history_db_ready=False
        _history_db_error=str(e)
        print(f"History write failed: {e}")
        return False

def save_selected_mode(mode,tasking_state=None):
    # A successful mode change must survive server restarts.
    with psycopg.connect(HISTORY_DATABASE_URL,connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO tracker_settings (id,mode,tasking_state) VALUES (1,%s,%s) ON CONFLICT (id) DO UPDATE SET mode=EXCLUDED.mode,tasking_state=EXCLUDED.tasking_state",(mode,tasking_state or state["tasking_state"]))

def restore_selected_mode():
    if not _history_db_ready:
        raise RuntimeError("Cannot safely restore tracker mode: database unavailable")
    with psycopg.connect(HISTORY_DATABASE_URL,connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT mode,tasking_state FROM tracker_settings WHERE id=1")
            row=cur.fetchone()
            if not row:
                # Migrate the last explicit user selection, never a server-start default.
                cur.execute("SELECT mode FROM tracker_history WHERE title='Mode changed' AND mode IN ('off','armed','geofence') ORDER BY id DESC LIMIT 1")
                row=cur.fetchone()
            restored=row[0] if row else "off"
            if restored not in ("off","armed","geofence"):
                raise RuntimeError("Saved tracker mode is invalid")
            cur.execute("INSERT INTO tracker_settings (id,mode) VALUES (1,%s) ON CONFLICT (id) DO NOTHING",(restored,))
    state["mode"]=restored
    state["tasking_state"]=row[1] if row and len(row)>1 and row[1] in ("ready","storage") else "ready"
    if state["tasking_state"]=="storage": state["mode"]="off"

def refresh_vitals(force=False):
    if not PARTICLE_ACCESS_TOKEN or not PARTICLE_DEVICE_ID:
        return
    now=time.time()
    if not force and now-state.get("_vitals_checked",0)<15:
        return
    state["_vitals_checked"]=now
    try:
        r=requests.get(
            f"https://api.particle.io/v1/diagnostics/{PARTICLE_DEVICE_ID}/last",
            headers={"Authorization":f"Bearer {PARTICLE_ACCESS_TOKEN}","Accept":"application/json"},
            timeout=12
        )
        if not r.ok:
            print(f"Particle vitals failed status={r.status_code} body={r.text[:240]}")
            return
        doc=r.json()
        diag=doc.get("diagnostics",{}) if isinstance(doc,dict) else {}
        vitals.add(diag)
        payload=diag.get("payload",{}) if isinstance(diag,dict) else {}
        device=payload.get("device",{}) if isinstance(payload,dict) else {}
        network=device.get("network",{}) if isinstance(device,dict) else {}
        signal=network.get("signal",{}) if isinstance(network,dict) else {}
        power=device.get("power",{}) if isinstance(device,dict) else {}
        battery=power.get("battery",{}) if isinstance(power,dict) else {}

        charge=battery.get("charge") if isinstance(battery,dict) else None
        if isinstance(charge,(int,float)):
            state["battery_percent"]=int(round(float(charge)))

        strength=signal.get("strength") if isinstance(signal,dict) else None
        quality=signal.get("quality") if isinstance(signal,dict) else None
        rat=signal.get("at") if isinstance(signal,dict) else None
        if isinstance(strength,(int,float)):
            state["lte"]=f"{float(strength):.0f}%"
        if isinstance(quality,(int,float)):
            state["lte_quality"]=f"{float(quality):.0f}%"
        if rat:
            state["network_type"]=str(rat)

        batt_state=str(battery.get("state","")).lower() if isinstance(battery,dict) else ""
        source=str(power.get("source","")).lower() if isinstance(power,dict) else ""
        if batt_state in ("charging","charged"):
            state["external_power"]=True
        elif batt_state in ("discharging","disconnected"):
            state["external_power"]=False
        elif source and source!="unknown":
            state["external_power"]=True

        updated=diag.get("updated_at") if isinstance(diag,dict) else None
        if updated:
            state["vitals_updated_at"]=updated
    except Exception as e:
        print(f"Particle vitals exception: {e}")
def note(t,d):
    ts=datetime.now(timezone.utc).isoformat()
    state["events"].insert(0,{"title":t,"detail":d,"time":ts})
    state["events"]=state["events"][:50]
    history_record("event",t,d,event_time=ts)
def timeline(stage,detail="",when=None,kind="info"):
    ts=when or datetime.now(timezone.utc).isoformat()
    state["timeline"].insert(0,{"stage":stage,"detail":detail,"time":ts,"kind":kind})
    state["timeline"]=state["timeline"][:500]
    history_record(kind or "timeline",stage,detail,event_time=ts)

def iso_from_event_time(v):
    if v is None:return None
    try:
        if isinstance(v,(int,float)) or str(v).replace(".","",1).isdigit():
            return datetime.fromtimestamp(float(v),timezone.utc).isoformat()
        return str(v)
    except Exception:
        return None

def seconds_between(a,b):
    try:
        aa=datetime.fromisoformat(str(a).replace("Z","+00:00"))
        bb=datetime.fromisoformat(str(b).replace("Z","+00:00"))
        return max(0,(bb-aa).total_seconds())
    except Exception:
        return None
def telegram_ready():
    state["telegram_configured"]=bool(TELEGRAM_BOT_TOKEN and state.get("telegram_chat_id"))
    return state["telegram_configured"]

def discover_telegram_chat():
    if state.get("telegram_chat_id"): return state["telegram_chat_id"]
    if not TELEGRAM_BOT_TOKEN: return None
    r=requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",timeout=10)
    r.raise_for_status()
    data=r.json()
    updates=data.get("result",[])
    print(f"Telegram getUpdates ok={data.get('ok')} count={len(updates)}")
    for upd in reversed(updates):
        msg=upd.get("message") or upd.get("channel_post") or upd.get("edited_message")
        if msg and msg.get("chat",{}).get("id"):
            state["telegram_chat_id"]=str(msg["chat"]["id"])
            print(f"Telegram chat discovered: {state['telegram_chat_id']}")
            break
    telegram_ready()
    return state.get("telegram_chat_id")

def send_alert(body,key,force=False):
    chat_id=discover_telegram_chat()
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        note("Alert not sent","Open the Telegram bot and press Start first")
        return False
    now=time.time()
    if not force and state["_key"]==key and now-state["_epoch"]<120:return False
    r=requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={"chat_id":chat_id,"text":body},timeout=10)
    if not r.ok:
        raise HTTPException(502,f"Telegram error: {r.text[:180]}")
    state["_key"],state["_epoch"]=key,now
    sent_at=datetime.now(timezone.utc).isoformat()
    state["last_alert_at"]=sent_at
    detail="Telegram accepted alert"
    if key.startswith("movement"):
        origin=state.get("_last_motion_device_time") or state.get("_last_motion_published_at") or state.get("_last_motion_webhook_at")
        delay=seconds_between(origin,sent_at) if origin else None
        if delay is not None:
            detail=f"Telegram accepted alert • {delay:.1f}s from device motion event"
    timeline("TELEGRAM ACCEPTED",detail,sent_at,"alert")
    note("Telegram alert sent",body[:120]);return True

def miles(a,b,c,d):
    r=3958.7613;p1,p2=math.radians(a),math.radians(c);dp=math.radians(c-a);dl=math.radians(d-b)
    x=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return r*2*math.atan2(math.sqrt(x),math.sqrt(1-x))
def alarm(reason,key):
    if state["mode"]=="off":return
    was_active=bool(state["alarm"])
    state["alarm"]=True;state["alarm_reason"]=reason
    if not was_active:
        state["_alarm_poll_next_at"]=0
        _alarm_poll_wake.set()
    timeline("ALARM TRIGGERED",reason,kind="alert")
    note("ALARM",reason)
    gps="GPS unavailable" if state["lat"] is None else f'{state["lat"]:.6f}, {state["lon"]:.6f}'
    map_link="" if state["lat"] is None else f"\nMap: https://maps.google.com/?q={state['lat']:.6f},{state['lon']:.6f}"
    send_alert(f"🚨 TRAILER ALERT\n{reason}\nMode: {state['mode'].upper()}\nSpeed: {state['speed_mph']:.0f} mph\nGPS: {gps}{map_link}\nOpen Rick's Trackers for live location.",key)
def auth_webhook(h):
    if not TOKEN:raise HTTPException(503,"Particle webhook token is not configured")
    if not h or not hmac.compare_digest(h,f"Bearer {TOKEN}"):raise HTTPException(401,"Invalid webhook authorization")


@app.get("/api/health")
def health(): return {"ok":True,"telegram_configured":telegram_ready()}

def ensure_development_device():
    if not PARTICLE_ACCESS_TOKEN or not PARTICLE_DEVICE_ID:
        print("Particle development-device setup skipped: control not configured")
        return False
    url=f"https://api.particle.io/v1/products/46064/devices/{PARTICLE_DEVICE_ID}"
    headers={"Authorization":f"Bearer {PARTICLE_ACCESS_TOKEN}","Accept":"application/json"}
    r=requests.put(url,headers=headers,data={"development":"true"},timeout=20)
    if not r.ok:
        print(f"Particle development-device PUT failed status={r.status_code} body={r.text[:500]}")
        return False
    doc=r.json() if r.text else {}
    print(f"Particle development-device status={r.status_code} development={doc.get('development')}")
    return bool(doc.get("development") is True or str(doc.get("development")).lower()=="true")

def get_particle_config():
    if not PARTICLE_ACCESS_TOKEN or not PARTICLE_DEVICE_ID:
        raise HTTPException(503,"Particle control is not configured")
    url=f"https://api.particle.io/v1/products/46064/config/{PARTICLE_DEVICE_ID}"
    headers={"Authorization":f"Bearer {PARTICLE_ACCESS_TOKEN}","Accept":"application/json"}
    r=requests.get(url,headers=headers,timeout=15)
    if not r.ok:
        raise HTTPException(r.status_code,f"Particle config lookup failed: {r.text[:220]}")
    return r.json()

def read_imu_motion():
    doc=get_particle_config()
    conf=doc.get("configuration") or {}
    current=conf.get("current") or {}
    pending=conf.get("pending") or {}
    current_motion=(current.get("imu_trig") or {}).get("motion")
    pending_motion=(pending.get("imu_trig") or {}).get("motion")
    if current_motion in ("low","medium","high","disable"):
        state["imu_sensitivity"]=current_motion
    if pending_motion in ("low","medium","high","disable"):
        state["imu_pending"]=pending_motion
    else:
        state["imu_pending"]=None
    return pending_motion or current_motion

def refresh_control_status(force=False):
    """Only acknowledged Particle configuration can confirm the requested mode."""
    now=time.time()
    if not force and now-state.get("_control_checked",0)<15:
        return
    state["_control_checked"]=now
    try:
        conf=get_particle_config().get("configuration") or {}
        current=conf.get("current") or {}
        pending=conf.get("pending") or {}
        motion=(current.get("imu_trig") or {}).get("motion")
        sleep=(current.get("sleep") or {}).get("mode")
        state["tracker_sleep"]=sleep
        state["tracker_update_interval_sec"]=(current.get("location") or {}).get("interval_max")
        state["imu_sensitivity"]=motion
        state["imu_pending"]=(pending.get("imu_trig") or {}).get("motion")
        desired="disable" if state["mode"]!="armed" else state.get("_armed_motion_sensitivity","high")
        minimum=60 if state["mode"]=="armed" else 3600
        desired_sleep="enable" if state["tasking_state"]=="storage" else "disable"
        acknowledged=(sleep==desired_sleep and motion==desired and
                      (current.get("location") or {}).get("interval_min")==minimum and
                      (current.get("location") or {}).get("interval_max")==3600)
        changing=any((pending.get(module) or {}).get(key,current.get(module,{}).get(key))!=value
                     for module,key,value in (("sleep","mode",desired_sleep),("imu_trig","motion",desired),("location","interval_min",minimum),("location","interval_max",3600)))
        result=requests.get(f"https://api.particle.io/v1/devices/{PARTICLE_DEVICE_ID}",
                            headers={"Authorization":f"Bearer {PARTICLE_ACCESS_TOKEN}"},timeout=10)
        result.raise_for_status()
        state["device_online"]=result.json().get("connected") is True
        state["mode_status"]="confirmed" if acknowledged and not changing else "pending"
        previous=state.get("readiness")
        if state["tasking_state"]=="storage":
            state["readiness"]="storage" if acknowledged and not changing else "storage_pending"
            state["readiness_message"]=("Storage sleep enabled. Checks in hourly. Press START UP before your next outing." if state["readiness"]=="storage" else "Storage requested. Waiting for tracker confirmation.")
        elif sleep=="disable" and (pending.get("sleep") or {}).get("mode","disable")=="disable" and state["device_online"]:
            state["readiness"]="ready"
            state["readiness_message"]="Tracker confirmed ready for tasking. It stays connected until you select STORAGE / SLEEP."
        else:
            state["readiness"]="startup_pending"
            state["readiness_message"]="Startup pending. Waiting for the tracker to connect and confirm it will stay awake; allow up to an hour, plus connection time."
        if state["readiness"]=="ready" and previous!="ready":
            timeline("READY FOR TASKING","Particle confirms tracker online and awake settings applied",kind="mode")
        state["mode_message"]=("Tracker settings confirmed" if state["mode_status"]=="confirmed" else
            ("Tracker is online; waiting for the requested mode settings to be confirmed." if state["device_online"] else
             "Waiting for tracker connection. A sleeping tracker checks in hourly."))
    except Exception:
        state["readiness"]="checking"
        state["device_online"]=None
        state["readiness_message"]="Readiness cannot be confirmed. Checking the tracker connection."
        state["mode_status"]="unknown"
        state["mode_message"]="Cannot confirm tracker settings. Check the connection and retry."


def deliver_config_now(updated):
    """Keep cloud configuration queued, but also deliver to an online tracker."""
    try:
        headers={"Authorization":f"Bearer {PARTICLE_ACCESS_TOKEN}"}
        device=requests.get(f"https://api.particle.io/v1/devices/{PARTICLE_DEVICE_ID}",headers=headers,timeout=10)
        device.raise_for_status()
        if device.json().get("connected") is not True:
            return False
        cfg={name:updated[name] for name in ("location","imu_trig","sleep") if name in updated}
        arg=requests.models.complexjson.dumps({"cmd":"set_cfg","cfg":cfg},separators=(",",":"))
        if len(arg.encode())>1024:
            return False
        result=requests.post(f"https://api.particle.io/v1/devices/{PARTICLE_DEVICE_ID}/cmd",
                             headers=headers,json={"arg":arg},timeout=20)
        result.raise_for_status()
        accepted=result.json().get("return_value")==0
        print(f"Direct tracker configuration accepted={accepted}")
        return accepted
    except Exception:
        print("Direct tracker configuration not confirmed; cloud request remains queued")
        return False

def set_tracker_power_profile(mode,tasking_state=None):
    """Apply Particle-side power behavior for app modes without changing secrets or firmware."""
    doc=get_particle_config()
    conf=doc.get("configuration") or {}
    current=conf.get("current") or {}
    if not isinstance(current,dict) or not current:
        raise HTTPException(502,"Particle current configuration is missing")

    updated=requests.models.complexjson.loads(requests.models.complexjson.dumps(current))
    updated.setdefault("sleep",{})
    updated.setdefault("location",{})
    updated.setdefault("imu_trig",{})

    if mode=="off":
        # Travel/off stops motion alarms but remains reachable for remote arming.
        updated["sleep"]["mode"]="disable"
        updated["location"]["interval_min"]=3600
        updated["location"]["interval_max"]=3600
        updated["imu_trig"]["motion"]="disable"
        sleep_state="disable"
        interval=3600
        motion="disable"
    elif mode=="armed":
        # Security mode: keep the tracker reachable and restore the user's armed sensitivity.
        desired=state.get("_armed_motion_sensitivity")
        if desired not in ("low","medium","high"):
            desired="high"
        updated["sleep"]["mode"]="disable"
        updated["imu_trig"]["motion"]=desired
        sleep_state="disable"
        interval=updated["location"].get("interval_max")
        motion=desired
    else:
        updated["sleep"]["mode"]="disable"
        desired=state.get("_armed_motion_sensitivity")
        if desired not in ("low","medium","high"):
            desired="high"
        updated["imu_trig"]["motion"]=desired
        sleep_state="disable"
        motion=desired
        interval=updated["location"].get("interval_max")

    if (tasking_state or state["tasking_state"])=="storage":
        updated["sleep"]["mode"]="enable"
        updated["imu_trig"]["motion"]="disable"
    updated["location"]["interval_min"]=60 if mode in ("armed","geofence") else 3600
    updated["location"]["interval_max"]=3600
    updated["location"]["lock_trigger"]=False
    url=f"https://api.particle.io/v1/products/46064/config/{PARTICLE_DEVICE_ID}"
    headers={"Authorization":f"Bearer {PARTICLE_ACCESS_TOKEN}","Accept":"application/json","Content-Type":"application/json"}
    r=requests.put(url,headers=headers,json=updated,timeout=20)
    if not r.ok:
        raise HTTPException(r.status_code,f"Particle power-profile update failed: {r.text[:260]}")

    deliver_config_now(updated)
    state["mode_status"]="pending"
    state["mode_message"]="Command submitted; waiting for the tracker to apply it."
    state["_control_checked"]=0
    note("Tracker power profile", "STORAGE: hourly wake-ups" if updated["sleep"]["mode"]=="enable" else
         f"{mode.upper()}: stays connected, hourly routine reports")
    return True

def set_imu_motion(sensitivity):
    doc=get_particle_config()
    conf=doc.get("configuration") or {}
    current=conf.get("current") or {}
    if not isinstance(current,dict) or not current:
        raise HTTPException(502,"Particle current configuration is missing")
    updated=requests.models.complexjson.loads(requests.models.complexjson.dumps(current))
    updated.setdefault("imu_trig",{})["motion"]=sensitivity
    url=f"https://api.particle.io/v1/products/46064/config/{PARTICLE_DEVICE_ID}"
    headers={"Authorization":f"Bearer {PARTICLE_ACCESS_TOKEN}","Accept":"application/json","Content-Type":"application/json"}
    r=requests.put(url,headers=headers,json=updated,timeout=20)
    if not r.ok:
        raise HTTPException(r.status_code,f"Particle IMU update failed: {r.text[:260]}")
    deliver_config_now({"imu_trig":updated["imu_trig"]})
    state["_armed_motion_sensitivity"]=sensitivity
    state["imu_sensitivity"]=sensitivity
    state["imu_pending"]=sensitivity
    time.sleep(1)
    actual=read_imu_motion()
    confirmed=(state.get("imu_sensitivity")==sensitivity)
    queued=(state.get("imu_pending")==sensitivity)
    if not (confirmed or queued):
        raise HTTPException(502,f"Particle did not accept {sensitivity} sensitivity")
    note("Motion sensitivity changed",sensitivity.upper() + (" (pending device apply)" if queued and not confirmed else ""))
    return sensitivity

@app.get("/api/trailer/imu-config")
def imu_config():
    doc=get_particle_config()
    conf=doc.get("configuration") or {}
    current=conf.get("current") or {}
    pending=conf.get("pending") or {}
    motion=((pending.get("imu_trig") or {}).get("motion")
            or (current.get("imu_trig") or {}).get("motion"))
    state["imu_sensitivity"]=motion
    return {"motion":motion,"current":current.get("imu_trig"),"pending":pending.get("imu_trig")}

@app.post("/api/trailer/imu-sensitivity")
def imu_sensitivity(x:ImuSensitivity):
    actual=set_imu_motion(x.sensitivity)
    return {"ok":True,"imu_sensitivity":actual,"imu_pending":state.get("imu_pending")}
@app.get("/api/trailer/status")
def status():
    telegram_ready()
    refresh_vitals()
    refresh_control_status()
    return pub()

@app.get("/api/homebase/history")
def homebase_history(hours:int=168,limit:int=1000):
    global _history_db_ready,_history_db_error
    limit=max(1,min(int(limit),5000))
    hours=max(0,min(int(hours),24*365*10))
    if not HISTORY_DATABASE_URL:
        return {"persistent":False,"error":"History database is not connected","items":[]}
    try:
        if not _history_db_ready and not history_init():
            return {"persistent":False,"error":_history_db_error,"items":[]}
        sql="""
            SELECT id,created_at,event_time,category,title,detail,mode,alarm,lat,lon,
                   speed_mph,battery_percent,external_power,lte,gps_fix
            FROM tracker_history
        """
        params=[]
        if hours>0:
            sql+=" WHERE created_at >= NOW() - make_interval(hours => %s)"
            params.append(hours)
        sql+=" ORDER BY created_at DESC LIMIT %s"
        params.append(limit)
        with psycopg.connect(HISTORY_DATABASE_URL,connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(sql,params)
                rows=cur.fetchall()
        keys=["id","created_at","event_time","category","title","detail","mode","alarm",
              "lat","lon","speed_mph","battery_percent","external_power","lte","gps_fix"]
        items=[]
        for row in rows:
            item=dict(zip(keys,row))
            for k in ("created_at","event_time"):
                if item.get(k) is not None:item[k]=item[k].isoformat()
            items.append(item)
        return {"persistent":True,"count":len(items),"hours":hours,"items":items}
    except Exception as e:
        _history_db_ready=False
        _history_db_error=str(e)
        print(f"History query failed: {e}")
        return {"persistent":False,"error":str(e),"items":[]}

@app.get("/api/homebase/summary")
def homebase_summary():
    return {
        "history_database_configured":bool(HISTORY_DATABASE_URL),
        "history_database_ready":bool(_history_db_ready),
        "history_database_error":_history_db_error,
        "live":pub()
    }
def request_fresh_location():
    """Request one fresh Tracker One location using the existing Particle get_loc command."""
    if not PARTICLE_ACCESS_TOKEN or not PARTICLE_DEVICE_ID:
        return {"ok":False,"error":"Particle control is not configured"}
    headers={
        "Authorization":f"Bearer {PARTICLE_ACCESS_TOKEN}",
        "Content-Type":"application/json",
        "Accept":"application/json"
    }
    try:
        cr=requests.post(
            f"https://api.particle.io/v1/devices/{PARTICLE_DEVICE_ID}/cmd",
            headers=headers,
            json={"arg":'{"cmd":"get_loc"}'},
            timeout=35
        )
        if not cr.ok:
            print(f"Particle fresh-location request failed status={cr.status_code} body={cr.text[:300]}")
            return {"ok":False,"status_code":cr.status_code,"error":cr.text[:220]}
        result=cr.json()
        rv=result.get("return_value")
        connected=result.get("connected",True)
        if rv not in (0,None):
            return {"ok":False,"online":bool(connected),"return_value":rv,
                    "error":f"Tracker rejected get_loc command (return value {rv})"}
        try:
            requests.post(
                f"https://api.particle.io/v1/diagnostics/{PARTICLE_DEVICE_ID}/update",
                headers={"Authorization":f"Bearer {PARTICLE_ACCESS_TOKEN}"},
                timeout=12
            )
        except Exception as e:
            print(f"Particle vitals refresh skipped: {e}")
        state["_vitals_checked"]=0
        return {"ok":True,"online":bool(connected),"requested":True,"return_value":rv}
    except Exception as e:
        print(f"Particle fresh-location exception: {e}")
        return {"ok":False,"error":str(e)}

def alarm_poll_worker():
    """Single per-process worker that requests a fresh location every 60s while alarmed."""
    print("Alarm location polling worker started")
    while not _alarm_poll_stop.is_set():
        if not state.get("alarm"):
            state["_alarm_poll_next_at"]=None
            _alarm_poll_wake.wait(1.0)
            _alarm_poll_wake.clear()
            continue

        now=time.monotonic()
        next_at=state.get("_alarm_poll_next_at")
        if next_at is None or now>=next_at:
            # Reserve the next slot before making the network call so repeated alarm
            # events cannot create overlapping requests.
            state["_alarm_poll_next_at"]=now+60.0
            result=request_fresh_location()
            if result.get("ok"):
                note("Alarm tracking","Fresh tracker location requested automatically")
                timeline("ALARM LOCATION POLL","Fresh Tracker One location requested",kind="tracking")
            else:
                print(f"Alarm location poll failed: {result.get('error')}")
            if state.get("alarm"):
                state["_alarm_poll_next_at"]=time.monotonic()+60.0
            else:
                state["_alarm_poll_next_at"]=None
            continue

        wait_for=max(0.1,min(1.0,next_at-now))
        _alarm_poll_wake.wait(wait_for)
        _alarm_poll_wake.clear()
    print("Alarm location polling worker stopped")

def ensure_alarm_poll_worker():
    """Start exactly one polling worker for this Render process."""
    global _alarm_poll_thread
    with _alarm_poll_thread_lock:
        if _alarm_poll_thread and _alarm_poll_thread.is_alive():
            return
        _alarm_poll_stop.clear()
        _alarm_poll_thread=threading.Thread(
            target=alarm_poll_worker,
            name="alarm-location-poller",
            daemon=True
        )
        _alarm_poll_thread.start()

def ping_after_arm():
    if state["mode"]=="armed" and state.get("mode_status")=="confirmed":
        try: request_fresh_location()
        except Exception: print("Initial arm location request failed")

@app.post("/api/trailer/tasking")
def tasking(x:Tasking):
    if not _mode_change_lock.acquire(blocking=False):
        raise HTTPException(409,"Another command is being submitted. Please wait.")
    try:
        target="ready" if x.action=="startup" else "storage"
        # Repeated Startup must never disarm an already active tracker.
        selected=state["mode"] if target=="ready" else "off"
        set_tracker_power_profile(selected,target)
        save_selected_mode(selected,target)
        state["tasking_state"]=target
        state["mode"]=selected
        if target=="storage":
            state["alarm"]=False
            state["alarm_reason"]=None
            state["_motion_started"]=None
            state["_alarm_poll_next_at"]=None
        state["readiness"]="startup_pending" if target=="ready" else "storage_pending"
        timeline("STARTUP REQUESTED" if target=="ready" else "STORAGE REQUESTED",
                 "Remain awake for tasking" if target=="ready" else "Alarms off; hourly sleep check-ins",kind="mode")
        refresh_control_status(force=True)
        return pub()
    finally:
        _mode_change_lock.release()

def setmode_once(x:Mode, background_tasks:BackgroundTasks):
    if not _mode_change_lock.acquire(blocking=False):
        raise HTTPException(409,"A mode change is already being submitted. Please wait.")
    try:
        refresh_control_status(force=True)
        if x.mode in ("armed","geofence") and state.get("readiness")!="ready":
            raise HTTPException(409,"Press START UP and wait for READY FOR TASKING before arming or selecting geofence.")
        timeline("MODE REQUESTED", "User requested "+x.mode.upper(), kind="mode")
        set_tracker_power_profile(x.mode)
        try:
            save_selected_mode(x.mode)
        except Exception:
            raise HTTPException(503,"Tracker profile may have been accepted, but the selected mode could not be saved. Please retry.")
        state["mode"]=x.mode
        if x.mode=="off":
            state["alarm"]=False
            state["alarm_reason"]=None
            state["_motion_started"]=None
            state["_alarm_poll_next_at"]=None
        refresh_control_status(force=True)
        note("Mode changed",x.mode+" • "+state["mode_message"])
        timeline("MODE CONFIRMED" if state["mode_status"]=="confirmed" else "MODE PENDING",
                 x.mode.upper()+" • "+state["mode_message"],kind="mode")
        if x.mode=="armed":
            background_tasks.add_task(ping_after_arm)
        return pub()
    finally:
        _mode_change_lock.release()
@app.post("/api/trailer/mode")
def setmode(x:Mode, background_tasks:BackgroundTasks):
    # A newer user request supersedes retries of an older request.
    request_id=object()
    state["_mode_request_id"]=request_id
    label="DISARM" if x.mode=="off" else x.mode.upper()
    state["command_warning"]=None
    for attempt in range(1,4):
        if state.get("_mode_request_id") is not request_id:
            raise HTTPException(409,"This command was replaced by a newer mode request.")
        try:
            result=setmode_once(x,background_tasks)
            if state.get("_mode_request_id") is request_id:
                state["command_warning"]=None
            if attempt>1:
                timeline(label+" RETRY ACCEPTED","Request accepted on attempt "+str(attempt)+"; see mode status for device confirmation",kind="mode")
            return pub()
        except Exception as exc:
            code=exc.status_code if isinstance(exc,HTTPException) else 502
            # Retry temporary service/network failures, never rejected inputs/authentication.
            retryable=code>=500 or code in (408,429)
            retry=retryable and attempt<3 and state.get("_mode_request_id") is request_id
            reason=("Service error (HTTP "+str(code)+")" if isinstance(exc,HTTPException) else "Connection or server error")
            message=label+" FAILED — "+reason+". Attempt "+str(attempt)+" of 3. "
            message+=("Retrying in 3 seconds." if retry else "Not confirmed. Please try again.")
            timeline(label+" FAILED",message,kind="warning")
            print("Mode command failure: mode="+x.mode+" attempt="+str(attempt)+" status="+str(code)+" type="+type(exc).__name__)
            if state.get("_mode_request_id") is request_id:
                state["command_warning"]=message
            if not retry:
                raise HTTPException(code,message)
            time.sleep(3)
            if state.get("_mode_request_id") is request_id:
                timeline(label+" RETRY","Automatic attempt "+str(attempt+1)+" of 3",kind="warning")

@app.post("/api/trailer/geofence")
def setgeo(x:Fence):
    state["geofence_ft"]=x.feet; note("Geofence changed",f"{x.feet} ft"); return pub()
@app.post("/api/trailer/live")
def setlive(x:Live):
    state["live"]=x.enabled; note("Live tracking request","Frequent updates requested" if x.enabled else "Normal updates requested"); return pub()
@app.post("/api/trailer/ping")
def ping_tracker():
    result=request_fresh_location()
    if not result.get("ok"):
        status_code=result.get("status_code",502)
        if not PARTICLE_DEVICE_ID:
            status_code=503
        elif not PARTICLE_ACCESS_TOKEN:
            status_code=503
        raise HTTPException(status_code if status_code < 500 else 502,
                            f"Particle location request failed: {result.get('error','unknown error')}")
    note("Tracker ping","Fresh location and vitals requested from Tracker One")
    return result

@app.post("/api/trailer/test-alert")
def testalert():
    if not TELEGRAM_BOT_TOKEN:raise HTTPException(503,"Telegram bot is not configured on the server")
    if not discover_telegram_chat():raise HTTPException(503,"Open your Telegram bot and send a message, then try again")
    send_alert("✅ RICK'S TRACKERS TEST\nTelegram alerts are working.",f"test-{time.time()}",True)
    return {"ok":True,"channel":"telegram"}

@app.post("/api/trailer/test-sms")
def testsms_compat():
    return testalert()

@app.post("/api/trailer/set-home")
def sethome():
    if state["lat"] is None or state["lon"] is None: raise HTTPException(409,"No live tracker position yet")
    state["home_lat"],state["home_lon"]=state["lat"],state["lon"]
    note("Home position set","Current tracker position")
    return pub()

@app.post("/api/trailer/clear-alarm")
def clear():
    state["alarm"]=False;state["alarm_reason"]=None
    state["_alarm_poll_next_at"]=None
    _alarm_poll_wake.set()
    note("Alarm cleared","Returned to monitoring • automatic location polling stopped")
    timeline("ALARM CLEARED","Automatic 60-second location polling stopped",kind="mode")
    return pub()
@app.post("/api/particle/webhook")
async def webhook(request:Request,authorization:Optional[str]=Header(default=None)):
    auth_webhook(authorization)
    monitor.record("webhook", "received")
    webhook_received_at=datetime.now(timezone.utc).isoformat()
    raw=await request.json()

    # Particle webhooks wrap the device event in "data"; Tracker firmware versions
    # can vary the exact nesting, so normalize several known native shapes.
    payload=raw
    if isinstance(raw,dict) and "data" in raw:
        data=raw.get("data")
        if isinstance(data,str):
            try: payload=requests.models.complexjson.loads(data)
            except Exception: payload={"raw":data}
        elif isinstance(data,(dict,list)):
            payload=data

    def find_loc(obj):
        if isinstance(obj,dict):
            loc=obj.get("loc")
            if isinstance(loc,dict) and ("lat" in loc or "latitude" in loc) and ("lon" in loc or "lng" in loc or "longitude" in loc):
                return loc,obj
            if ("lat" in obj or "latitude" in obj) and ("lon" in obj or "lng" in obj or "longitude" in obj):
                return obj,obj
            for v in obj.values():
                found=find_loc(v)
                if found:return found
        elif isinstance(obj,list):
            for v in obj:
                found=find_loc(v)
                if found:return found
        return None

    def scalar(v,*keys):
        if isinstance(v,(int,float,str)): return v
        if isinstance(v,dict):
            for k in keys:
                if k in v and isinstance(v[k],(int,float,str)): return v[k]
        return None

    # Firmware-tolerant motion detection. Newer Tracker firmware may publish
    # movement as a separate event without coordinates, or move the trigger
    # deeper in the payload. Collect motion markers recursively instead of
    # requiring a location object to contain trig/trigger.
    def motion_markers(obj):
        out=[]
        if isinstance(obj,dict):
            for k,v in obj.items():
                kl=str(k).lower()
                if kl in ("trig","trigger","triggers","reason","event","event_name","type","name"):
                    if isinstance(v,str):
                        out.append(v.lower())
                    elif isinstance(v,list):
                        out.extend(str(x).lower() for x in v if isinstance(x,(str,int,float)))
                    elif isinstance(v,(int,float)):
                        out.append(str(v).lower())
                out.extend(motion_markers(v))
        elif isinstance(obj,list):
            for v in obj:
                out.extend(motion_markers(v))
        return out

    markers=motion_markers(payload)
    if isinstance(raw,dict) and raw.get("event"):
        markers.append(str(raw.get("event")).lower())
    motion_event=any(
        m=="imu_m" or m.startswith("imu_") or "motion" in m or
        m in ("move","movement","moving")
        for m in markers
    )

    report_time=None
    report_source="payload timestamp"
    found=find_loc(payload)

    # A motion-only Particle event is enough to trip ARMED mode immediately.
    # Do not wait for GPS; the alarm worker will begin requesting fresh location.
    if motion_event and not found:
        event_name=raw.get("event") if isinstance(raw,dict) else None
        published_at=raw.get("published_at") if isinstance(raw,dict) else None
        state["_last_motion_device_time"]=None
        state["_last_motion_published_at"]=published_at
        state["_last_motion_webhook_at"]=webhook_received_at
        detail="Tracker reported motion"
        if event_name:
            detail+=f" • event={event_name}"
        if markers:
            detail+=" • markers="+", ".join(dict.fromkeys(markers))
        timeline("DEVICE MOTION EVENT",detail,published_at or webhook_received_at,"motion")
        print(f"Particle motion-only event={event_name} markers={markers} mode={state['mode']}")
        if state["mode"]=="armed":
            alarm("Movement detected by Tracker One","movement")
        return {"ok":True,"motion":True,"alarm":bool(state.get("alarm"))}
    if found:
        loc,container=found
        lat=loc.get("lat",loc.get("latitude"))
        lon=loc.get("lon",loc.get("lng",loc.get("longitude")))
        try: lat=float(lat); lon=float(lon)
        except Exception:
            return {"ok":True,"ignored":"location coordinates not numeric"}

        spd=loc.get("spd",loc.get("speed",container.get("spd",container.get("speed",0)))) or 0
        try: speed_mph=float(spd)*2.2369362920544
        except Exception: speed_mph=0

        batt=loc.get("batt",container.get("batt",container.get("battery")))
        batt=scalar(batt,"soc","percent","pct","charge")
        try: batt_pct=None if batt is None else int(round(float(batt)))
        except Exception: batt_pct=None

        cell=loc.get("cell",container.get("cell",container.get("cellular")))
        cell=scalar(cell,"strength","quality","percent","pct")
        try: lte=None if cell is None else f"{float(cell):.0f}%"
        except Exception: lte=None

        trig=container.get("trig",container.get("trigger",container.get("triggers",[])))
        if isinstance(trig,str): triggers=[trig.lower()]
        elif isinstance(trig,list): triggers=[str(t).lower() for t in trig]
        else: triggers=[]
        imu_motion=motion_event or any(
            t=="imu_m" or t.startswith("imu_") or "motion" in t or t in ("move","movement","moving")
            for t in triggers
        )
        print(f"Particle loc triggers={triggers} imu_motion={imu_motion} mode={state['mode']}")

        published_at=raw.get("published_at") if isinstance(raw,dict) else None
        event_time=container.get("time",container.get("timestamp"))
        device_event_time=iso_from_event_time(event_time)
        timestamp=published_at or device_event_time
        report_time=device_event_time or published_at
        report_source="tracker timestamp" if device_event_time else "Particle publish timestamp (tracker time unavailable)"

        lock=loc.get("lck",loc.get("lock",loc.get("fix",True)))
        x=Event(
            lat=lat,lon=lon,speed_mph=speed_mph,
            battery_percent=batt_pct,
            external_power=None,
            lte=lte,
            gps_fix=bool(lock),
            motion=imu_motion,
            alarm=False,alarm_reason=None,timestamp=timestamp
        )
        if imu_motion:
            state["_last_motion_device_time"]=device_event_time
            state["_last_motion_published_at"]=published_at
            state["_last_motion_webhook_at"]=webhook_received_at
            if device_event_time:
                timeline("DEVICE MOTION EVENT","Tracker reported IMU motion trigger: "+", ".join(triggers),device_event_time,"motion")
            if published_at:
                d=seconds_between(device_event_time,published_at) if device_event_time else None
                extra="" if d is None else f" • {d:.1f}s after device event"
                timeline("PARTICLE CLOUD","Motion/location event published"+extra,published_at,"cloud")
            d=seconds_between(published_at or device_event_time,webhook_received_at)
            extra="" if d is None else f" • {d:.1f}s after Particle publish"
            timeline("WEBHOOK RECEIVED","Rick's Trackers received Particle event"+extra,webhook_received_at,"server")
    else:
        # Keep custom/test payload support, but don't make Particle disable/retry a
        # valid webhook just because a non-location event has a different schema.
        try: x=Event(**payload)
        except Exception:
            event_name=raw.get("event") if isinstance(raw,dict) else None
            print(f"Particle event ignored: event={event_name} payload_type={type(payload).__name__}")
            return {"ok":True,"ignored":"non-location or unsupported Particle event"}

    if report_time or x.timestamp:
        timeline("GPS LOCATION REPORTED" if x.gps_fix else "LOCATION REPORTED", f"{x.lat:.6f}, {x.lon:.6f} • {report_source}; not a matched ping response", report_time or x.timestamp, "location")
    timeline("LOCATION RECEIVED", f"{x.lat:.6f}, {x.lon:.6f} • server receipt", webhook_received_at, "server")
    monitor.record("location", "received")
    prev=state["external_power"]
    for k in ["lat","lon","speed_mph","battery_percent","external_power","lte","gps_fix"]:
        v=getattr(x,k)
        if v is not None: state[k]=v
    state["last_seen"]=x.timestamp or datetime.now(timezone.utc).isoformat()
    state["route"].append({"lat":x.lat,"lon":x.lon});state["route"]=state["route"][-200:]
    history_record(
        "location","LOCATION UPDATE",
        "Motion trigger" if x.motion else "Tracker location received",
        event_time=state["last_seen"],lat=x.lat,lon=x.lon,speed_mph=x.speed_mph,
        battery_percent=x.battery_percent,external_power=x.external_power,lte=x.lte,gps_fix=x.gps_fix
    )
    if state["home_lat"] is None:
        state["home_lat"],state["home_lon"]=x.lat,x.lon
        note("Home position set","First valid tracker position")

    if x.alarm: alarm(x.alarm_reason or "Tracker alarm","device-alarm")

    # Native Tracker motion trigger. This is immediate on a Particle IMU movement publish.
    if state["mode"]=="armed" and x.motion:
        alarm("Movement detected by Tracker One","movement")

    # Speed-based fallback if multiple frequent points show sustained movement.
    if state["mode"]=="armed":
        moving=bool(x.speed_mph>=1.0)
        if moving:
            if state["_motion_started"] is None: state["_motion_started"]=time.time()
            elif time.time()-state["_motion_started"]>=10: alarm("Movement detected for 10 seconds","movement-10s")
        else:
            state["_motion_started"]=None
    else:
        state["_motion_started"]=None

    if state["mode"] in ("armed","geofence") and state["home_lat"] is not None:
        if miles(state["home_lat"],state["home_lon"],x.lat,x.lon)*5280>state["geofence_ft"]:
            alarm(f"Trailer left the {state['geofence_ft']} ft geofence",f"geo-{state['geofence_ft']}")

    if prev is True and x.external_power is False and state["mode"]!="off":
        alarm("External tracker power disconnected","power-loss")
    return {"ok":True,"lat":x.lat,"lon":x.lon,"speed_mph":round(x.speed_mph,1)}

# Instrument existing calls so manual and automatic pings share the same counters.
_original_location_request = request_fresh_location
def request_fresh_location():
    timeline("PING REQUESTED", "Server began requesting a fresh location", kind="tracking")
    try:
        result = _original_location_request()
        timeline("PING ACCEPTED" if result.get("ok") else "PING FAILED", "Command response received; GPS report arrives separately" if result.get("ok") else "Particle did not confirm the location request", kind="tracking")
        monitor.record("ping", "success" if result.get("ok") else "failure")
        return result
    except Exception:
        timeline("PING FAILED", "No successful command response", kind="tracking")
        monitor.record("ping", "failure")
        raise

_original_send_alert = send_alert
def send_alert(*args, **kwargs):
    try:
        result = _original_send_alert(*args, **kwargs)
        monitor.record("telegram", "success" if result else "skipped")
        return result
    except Exception:
        timeline("TELEGRAM FAILED", "Telegram did not confirm acceptance", kind="alert")
        monitor.record("telegram", "failure")
        raise

@app.get("/api/homebase/console")
def homebase_console():
    monitor.check(requests.get, PARTICLE_ACCESS_TOKEN, PARTICLE_DEVICE_ID,
                  TELEGRAM_BOT_TOKEN, state.get("telegram_chat_id"))
    result = monitor.snapshot()
    result["render"] = {"status": "responding", "uptime_sec": int(time.time()-monitor.started),
                        "commit": os.getenv("RENDER_GIT_COMMIT", "")[:7]}
    result["last_location_at"] = state.get("last_seen")
    result["last_alert_at"] = state.get("last_alert_at")
    result["vitals"] = vitals.snapshot()
    result["timeline"] = list(state["timeline"])
    result["database"] = "ready" if _history_db_ready else "unavailable" if HISTORY_DATABASE_URL else "not_configured"
    return result

@app.on_event("startup")
def startup_discover():
    history_init()
    restore_selected_mode()
    history_record("server","SERVER START","Rick's Trackers backend started")
    ensure_alarm_poll_worker()
    try: discover_telegram_chat()
    except Exception as e: print(f"Telegram startup discovery skipped: {e}")
    try:
        dev_ok=ensure_development_device()
        print(f"Particle development-device verify ok={dev_ok}")
        if dev_ok:
            time.sleep(1)
            refresh_control_status(force=True)
            print(f"Particle control status={state['mode_status']}")
    except Exception as e:
        print(f"Particle IMU setup exception: {e}")

@app.on_event("shutdown")
def shutdown_alarm_poll_worker():
    _alarm_poll_stop.set()
    _alarm_poll_wake.set()

@app.get("/homebase")
def homebase_page():
    return FileResponse("homebase.html")

@app.get("/homebase/")
def homebase_page_slash():
    return FileResponse("homebase.html")

app.mount("/",StaticFiles(directory=".",html=True),name="static")
