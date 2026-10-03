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
state={"mode":"armed","geofence_ft":1000,"live":False,"alarm":False,"alarm_reason":None,"lat":None,"lon":None,"home_lat":None,"home_lon":None,"speed_mph":0,"battery_percent":None,"external_power":None,"lte":None,"gps_fix":None,"last_seen":None,"route":[],"events":[],"telegram_configured":False,"last_alert_at":None,"telegram_chat_id":TELEGRAM_CHAT_ID or None,"_key":None,"_epoch":0,"_motion_started":None}

class Mode(BaseModel): mode:Literal["armed","geofence","off"]
class Fence(BaseModel): feet:Literal[100,500,1000]
class Live(BaseModel): enabled:bool
class Event(BaseModel):
    lat:float; lon:float; speed_mph:float=0; battery_percent:Optional[int]=None
    external_power:Optional[bool]=None; lte:Optional[str]=None; gps_fix:bool=True
    motion:bool=False; alarm:bool=False; alarm_reason:Optional[str]=None; timestamp:Optional[str]=None

def pub(): return {k:v for k,v in state.items() if not k.startswith("_")}
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
@app.get("/api/trailer/status")
def status(): telegram_ready(); return pub()
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

    # Particle webhooks normally wrap the actual event JSON in the "data" field.
    payload=raw
    if isinstance(raw,dict) and "data" in raw:
        data=raw.get("data")
        if isinstance(data,str):
            try: payload=requests.models.complexjson.loads(data)
            except Exception: payload={"raw":data}
        elif isinstance(data,dict):
            payload=data

    # Native Tracker One / Tracker Edge loc and loc-enhanced event.
    if isinstance(payload,dict) and isinstance(payload.get("loc"),dict):
        loc=payload["loc"]
        if "lat" not in loc or "lon" not in loc:
            return {"ok":True,"ignored":"no valid location yet"}
        lat=float(loc["lat"]); lon=float(loc["lon"])
        spd=loc.get("spd",loc.get("speed",0)) or 0
        speed_mph=float(spd)*2.2369362920544
        batt=loc.get("batt")
        cell=loc.get("cell")
        triggers=[str(t) for t in (payload.get("trig") or [])]
        timestamp=raw.get("published_at") if isinstance(raw,dict) else None
        if not timestamp and payload.get("time"):
            try: timestamp=datetime.fromtimestamp(float(payload["time"]),timezone.utc).isoformat()
            except Exception: timestamp=None
        x=Event(
            lat=lat,lon=lon,speed_mph=speed_mph,
            battery_percent=None if batt is None else int(round(float(batt))),
            external_power=None,
            lte=None if cell is None else f"{float(cell):.0f}%",
            gps_fix=bool(loc.get("lck",1)),
            motion=("imu_m" in triggers or "radius" in triggers),
            alarm=False,
            alarm_reason=None,
            timestamp=timestamp
        )
    else:
        # Also accept our direct/custom event format.
        try: x=Event(**payload)
        except Exception as e: raise HTTPException(422,f"Unsupported Particle payload: {e}")

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

app.mount("/",StaticFiles(directory=".",html=True),name="static")
