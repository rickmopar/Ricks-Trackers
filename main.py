import os, math, time, hmac
from datetime import datetime, timezone
from typing import Literal, Optional
from fastapi import FastAPI, HTTPException, Header
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from twilio.rest import Client

app=FastAPI(title="Rick's Trackers")
TOKEN=os.getenv("PARTICLE_WEBHOOK_TOKEN","")
SID=os.getenv("TWILIO_ACCOUNT_SID","")
AUTH=os.getenv("TWILIO_AUTH_TOKEN","")
FROM=os.getenv("TWILIO_FROM_NUMBER","")
TO=os.getenv("ALERT_TO_NUMBER","")
state={"mode":"armed","geofence_ft":1000,"live":False,"alarm":False,"alarm_reason":None,"lat":None,"lon":None,"home_lat":None,"home_lon":None,"speed_mph":0,"battery_percent":None,"external_power":None,"lte":None,"gps_fix":None,"last_seen":None,"route":[],"events":[],"sms_configured":False,"last_sms_at":None,"_key":None,"_epoch":0}

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
def sms_ready():
    state["sms_configured"]=all([SID,AUTH,FROM,TO]); return state["sms_configured"]
def send_sms(body,key,force=False):
    if not sms_ready(): note("SMS not sent","Twilio settings are not configured"); return False
    now=time.time()
    if not force and state["_key"]==key and now-state["_epoch"]<120:return False
    Client(SID,AUTH).messages.create(body=body,from_=FROM,to=TO)
    state["_key"],state["_epoch"]=key,now;state["last_sms_at"]=datetime.now(timezone.utc).isoformat()
    note("Text alert sent",body[:120]);return True
def miles(a,b,c,d):
    r=3958.7613;p1,p2=math.radians(a),math.radians(c);dp=math.radians(c-a);dl=math.radians(d-b)
    x=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return r*2*math.atan2(math.sqrt(x),math.sqrt(1-x))
def alarm(reason,key):
    if state["mode"]=="off":return
    state["alarm"]=True;state["alarm_reason"]=reason;note("ALARM",reason)
    gps="GPS unavailable" if state["lat"] is None else f'{state["lat"]:.6f}, {state["lon"]:.6f}'
    send_sms(f"TRAILER ALERT\n{reason}\nMode: {state['mode'].upper()}\nSpeed: {state['speed_mph']:.0f} mph\nGPS: {gps}\nOpen Rick's Trackers for live location.",key)
def auth_webhook(h):
    if not TOKEN:raise HTTPException(503,"Particle webhook token is not configured")
    if not h or not hmac.compare_digest(h,f"Bearer {TOKEN}"):raise HTTPException(401,"Invalid webhook authorization")

@app.get("/api/health")
def health(): return {"ok":True,"sms_configured":sms_ready()}
@app.get("/api/trailer/status")
def status(): sms_ready();return pub()
@app.post("/api/trailer/mode")
def setmode(x:Mode):
    state["mode"]=x.mode
    if x.mode=="off":state["alarm"]=False;state["alarm_reason"]=None
    note("Mode changed",x.mode);return pub()
@app.post("/api/trailer/geofence")
def setgeo(x:Fence): state["geofence_ft"]=x.feet;note("Geofence changed",f"{x.feet} ft");return pub()
@app.post("/api/trailer/live")
def setlive(x:Live): state["live"]=x.enabled;note("Live tracking","Frequent updates" if x.enabled else "Normal updates");return pub()
@app.post("/api/trailer/test-sms")
def testsms():
    if not sms_ready():raise HTTPException(503,"Twilio SMS is not configured on the server")
    send_sms("RICK'S TRACKERS TEST\nText alerts are working.",f"test-{time.time()}",True);return {"ok":True}
@app.post("/api/particle/webhook")
def webhook(x:Event,authorization:Optional[str]=Header(default=None)):
    auth_webhook(authorization);prev=state["external_power"]
    for k in ["lat","lon","speed_mph","battery_percent","external_power","lte","gps_fix"]:state[k]=getattr(x,k)
    state["last_seen"]=x.timestamp or datetime.now(timezone.utc).isoformat()
    state["route"].append({"lat":x.lat,"lon":x.lon});state["route"]=state["route"][-200:]
    if state["home_lat"] is None:state["home_lat"],state["home_lon"]=x.lat,x.lon;note("Home position set","First valid tracker position")
    if x.alarm:alarm(x.alarm_reason or "Tracker alarm","device-alarm")
    if state["mode"] in ("armed","geofence") and state["home_lat"] is not None:
        if miles(state["home_lat"],state["home_lon"],x.lat,x.lon)*5280>state["geofence_ft"]:alarm(f"Trailer left the {state['geofence_ft']} ft geofence",f"geo-{state['geofence_ft']}")
    if prev is True and x.external_power is False and state["mode"]!="off":alarm("External tracker power disconnected","power-loss")
    return {"ok":True}
@app.post("/api/trailer/clear-alarm")
def clear(): state["alarm"]=False;state["alarm_reason"]=None;note("Alarm cleared","Returned to monitoring");return pub()

app.mount("/",StaticFiles(directory=".",html=True),name="static")
