import os, math, time, hmac, requests
from datetime import datetime, timezone
from typing import Literal, Optional
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app=FastAPI(title="Rick's Trackers")
TOKEN=os.getenv("PARTICLE_WEBHOOK_TOKEN","")
TELEGRAM_BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN","")
TELEGRAM_CHAT_ID=os.getenv("TELEGRAM_CHAT_ID","")
PARTICLE_ACCESS_TOKEN=os.getenv("PARTICLE_ACCESS_TOKEN","")
PARTICLE_DEVICE_ID=os.getenv("PARTICLE_DEVICE_ID","")
state={"particle_control_configured":bool(PARTICLE_ACCESS_TOKEN and PARTICLE_DEVICE_ID),"mode":"armed","geofence_ft":1000,"live":False,"alarm":False,"alarm_reason":None,"lat":None,"lon":None,"home_lat":None,"home_lon":None,"speed_mph":0,"battery_percent":None,"external_power":None,"lte":None,"lte_quality":None,"network_type":None,"gps_fix":None,"last_seen":None,"vitals_updated_at":None,"route":[],"events":[],"telegram_configured":False,"last_alert_at":None,"telegram_chat_id":TELEGRAM_CHAT_ID or None,"_key":None,"_epoch":0,"_motion_started":None,"_vitals_checked":0}

class Mode(BaseModel): mode:Literal["armed","geofence","off"]
class Fence(BaseModel): feet:Literal[100,500,1000]
class Live(BaseModel): enabled:bool
class Event(BaseModel):
    lat:float; lon:float; speed_mph:float=0; battery_percent:Optional[int]=None
    external_power:Optional[bool]=None; lte:Optional[str]=None; gps_fix:bool=True
    motion:bool=False; alarm:bool=False; alarm_reason:Optional[str]=None; timestamp:Optional[str]=None

def pub(): return {k:v for k,v in state.items() if not k.startswith("_")}

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
    state["events"].insert(0,{"title":t,"detail":d,"time":datetime.now(timezone.utc).isoformat()})
    state["events"]=state["events"][:50]
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
    state["last_alert_at"]=datetime.now(timezone.utc).isoformat()
    note("Telegram alert sent",body[:120]);return True

def miles(a,b,c,d):
    r=3958.7613;p1,p2=math.radians(a),math.radians(c);dp=math.radians(c-a);dl=math.radians(d-b)
    x=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return r*2*math.atan2(math.sqrt(x),math.sqrt(1-x))
def alarm(reason,key):
    if state["mode"]=="off":return
    state["alarm"]=True;state["alarm_reason"]=reason;note("ALARM",reason)
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

def ensure_imu_motion():
    if not PARTICLE_ACCESS_TOKEN or not PARTICLE_DEVICE_ID:
        print("Particle IMU setup skipped: control not configured")
        return False
    url=f"https://api.particle.io/v1/products/46064/config/{PARTICLE_DEVICE_ID}"
    headers={"Authorization":f"Bearer {PARTICLE_ACCESS_TOKEN}","Accept":"application/json"}
    r=requests.get(url,headers=headers,timeout=15)
    if not r.ok:
        print(f"Particle IMU GET failed status={r.status_code} body={r.text[:300]}")
        return False
    doc=r.json()
    current=((doc.get("configuration") or {}).get("current") or {})
    if not isinstance(current,dict):
        print("Particle IMU setup failed: current config missing")
        return False
    imu=current.get("imu_trig") if isinstance(current.get("imu_trig"),dict) else {}
    if imu.get("motion")=="medium":
        print("Particle IMU motion already medium")
        return True

    updated=requests.models.complexjson.loads(requests.models.complexjson.dumps(current))
    updated.setdefault("imu_trig",{})["motion"]="medium"
    p=requests.put(
        url,
        headers={**headers,"Content-Type":"application/json"},
        json=updated,
        timeout=20
    )
    if not p.ok:
        print(f"Particle IMU PUT failed status={p.status_code} body={p.text[:500]}")
        return False
    print(f"Particle IMU PUT accepted status={p.status_code}")

    time.sleep(2)
    v=requests.get(url,headers=headers,timeout=15)
    if not v.ok:
        print(f"Particle IMU verify GET failed status={v.status_code} body={v.text[:300]}")
        return False
    vdoc=v.json()
    conf=vdoc.get("configuration") or {}
    cur=((conf.get("current") or {}).get("imu_trig") or {}).get("motion")
    pend=((conf.get("pending") or {}).get("imu_trig") or {}).get("motion")
    ok=(cur=="medium" or pend=="medium")
    print(f"Particle IMU verify current={cur} pending={pend} ok={ok}")
    return ok

@app.get("/api/trailer/imu-config")
def imu_config():
    if not PARTICLE_ACCESS_TOKEN or not PARTICLE_DEVICE_ID:
        raise HTTPException(503,"Particle control is not configured")
    r=requests.get(
        f"https://api.particle.io/v1/products/46064/config/{PARTICLE_DEVICE_ID}",
        headers={"Authorization":f"Bearer {PARTICLE_ACCESS_TOKEN}","Accept":"application/json"},
        timeout=15
    )
    if not r.ok:
        raise HTTPException(r.status_code,f"Particle config lookup failed: {r.text[:220]}")
    cfg=r.json()
    return {
        "imu_trig":cfg.get("imu_trig"),
        "location":cfg.get("location"),
        "geofence":cfg.get("geofence")
    }
@app.get("/api/trailer/status")
def status():
    telegram_ready()
    refresh_vitals()
    return pub()
@app.post("/api/trailer/mode")
def setmode(x:Mode):
    state["mode"]=x.mode
    if x.mode=="off":state["alarm"]=False;state["alarm_reason"]=None
    note("Mode changed",x.mode);return pub()
@app.post("/api/trailer/geofence")
def setgeo(x:Fence):
    state["geofence_ft"]=x.feet; note("Geofence changed",f"{x.feet} ft"); return pub()
@app.post("/api/trailer/live")
def setlive(x:Live):
    state["live"]=x.enabled; note("Live tracking request","Frequent updates requested" if x.enabled else "Normal updates requested"); return pub()
@app.post("/api/trailer/ping")
def ping_tracker():
    if not PARTICLE_DEVICE_ID:
        raise HTTPException(503,"Particle device ID is not configured")
    if not PARTICLE_ACCESS_TOKEN:
        raise HTTPException(503,"Particle API token is not configured yet")
    headers={
        "Authorization":f"Bearer {PARTICLE_ACCESS_TOKEN}",
        "Content-Type":"application/json",
        "Accept":"application/json"
    }
    cr=requests.post(
        f"https://api.particle.io/v1/devices/{PARTICLE_DEVICE_ID}/cmd",
        headers=headers,
        json={"arg":'{"cmd":"get_loc"}'},
        timeout=35
    )
    if not cr.ok:
        print(f"Particle cmd failed status={cr.status_code} body={cr.text[:300]}")
        raise HTTPException(cr.status_code if cr.status_code < 500 else 502,
                            f"Particle location request failed: {cr.text[:220]}")
    result=cr.json()
    rv=result.get("return_value")
    connected=result.get("connected",True)
    if rv not in (0,None):
        raise HTTPException(502,f"Tracker rejected get_loc command (return value {rv})")
    try:
        requests.post(
            f"https://api.particle.io/v1/diagnostics/{PARTICLE_DEVICE_ID}/update",
            headers={"Authorization":f"Bearer {PARTICLE_ACCESS_TOKEN}"},
            timeout=12
        )
    except Exception as e:
        print(f"Particle vitals refresh request skipped: {e}")
    state["_vitals_checked"]=0
    note("Tracker ping","Fresh location and vitals requested from Tracker One")
    return {"ok":True,"online":bool(connected),"requested":True,"return_value":rv}

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
    note("Alarm cleared","Returned to monitoring")
    return pub()
@app.post("/api/particle/webhook")
async def webhook(request:Request,authorization:Optional[str]=Header(default=None)):
    auth_webhook(authorization)
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

    found=find_loc(payload)
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
        if isinstance(trig,str): triggers=[trig]
        elif isinstance(trig,list): triggers=[str(t) for t in trig]
        else: triggers=[]

        timestamp=raw.get("published_at") if isinstance(raw,dict) else None
        event_time=container.get("time",container.get("timestamp"))
        if not timestamp and event_time:
            try:
                if isinstance(event_time,(int,float)) or str(event_time).replace(".","",1).isdigit():
                    timestamp=datetime.fromtimestamp(float(event_time),timezone.utc).isoformat()
                else: timestamp=str(event_time)
            except Exception: timestamp=None

        lock=loc.get("lck",loc.get("lock",loc.get("fix",True)))
        x=Event(
            lat=lat,lon=lon,speed_mph=speed_mph,
            battery_percent=batt_pct,
            external_power=None,
            lte=lte,
            gps_fix=bool(lock),
            motion=any(t in ("imu_m","radius","motion","move") for t in triggers),
            alarm=False,alarm_reason=None,timestamp=timestamp
        )
    else:
        # Keep custom/test payload support, but don't make Particle disable/retry a
        # valid webhook just because a non-location event has a different schema.
        try: x=Event(**payload)
        except Exception:
            event_name=raw.get("event") if isinstance(raw,dict) else None
            print(f"Particle event ignored: event={event_name} payload_type={type(payload).__name__}")
            return {"ok":True,"ignored":"non-location or unsupported Particle event"}

    prev=state["external_power"]
    for k in ["lat","lon","speed_mph","battery_percent","external_power","lte","gps_fix"]:
        v=getattr(x,k)
        if v is not None: state[k]=v
    state["last_seen"]=x.timestamp or datetime.now(timezone.utc).isoformat()
    state["route"].append({"lat":x.lat,"lon":x.lon});state["route"]=state["route"][-200:]
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
@app.on_event("startup")
def startup_discover():
    try: discover_telegram_chat()
    except Exception as e: print(f"Telegram startup discovery skipped: {e}")
    try:
        dev_ok=ensure_development_device()
        print(f"Particle development-device verify ok={dev_ok}")
        if dev_ok:
            time.sleep(1)
            ensure_imu_motion()
    except Exception as e:
        print(f"Particle IMU setup exception: {e}")

app.mount("/",StaticFiles(directory=".",html=True),name="static")
