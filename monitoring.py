"""Bounded operational telemetry. Never stores credentials, payloads or locations."""
import time
import threading
from collections import deque
from datetime import datetime, timezone

class Monitor:
    def __init__(self, clock=time.time):
        self.clock = clock
        self.started = clock()
        self.lock = threading.Lock()
        self.check_lock = threading.Lock()
        self.events = deque(maxlen=500)
        self.buckets = {}
        self.services = {}
        self.checked = None

    def record(self, kind, outcome):
        now = self.clock()
        with self.lock:
            minute = int(now // 60) * 60
            bucket = self.buckets.setdefault(minute, {})
            key = kind + "_" + outcome
            bucket[key] = bucket.get(key, 0) + 1
            self.events.append({"time": now, "kind": kind, "outcome": outcome})
            self.buckets = {t: b for t, b in self.buckets.items() if t >= minute - 3540}

    def check(self, get, particle_token, device_id, telegram_token, chat_ready):
        # One check per minute across all viewers; never issues tracker commands.
        with self.check_lock:
            now = self.clock()
            if self.checked is not None and now - self.checked < 60:
                return
            services = {}
            for name, configured, url, headers in [
                ("particle", bool(particle_token and device_id),
                 f"https://api.particle.io/v1/devices/{device_id}",
                 {"Authorization": f"Bearer {particle_token}"}),
                ("telegram", bool(telegram_token),
                 f"https://api.telegram.org/bot{telegram_token}/getMe", {})
            ]:
                result = {"status": "not_configured", "checked_at": now}
                if configured:
                    try:
                        response = get(url, headers=headers, timeout=5)
                        if not response.ok:
                            result["status"] = "error"
                            result["http_status"] = response.status_code
                        else:
                            doc = response.json()
                            if name == "particle":
                                valid = doc.get("id") == device_id
                                result["status"] = "connected" if valid else "error"
                                result["device_online"] = doc.get("connected") if valid and isinstance(doc.get("connected"), bool) else None
                                result["last_heard"] = doc.get("last_heard") if valid else None
                            else:
                                result["status"] = "connected" if doc.get("ok") is True else "error"
                                result["chat_configured"] = bool(chat_ready)
                    except Exception:
                        result["status"] = "unreachable"
                services[name] = result
            with self.lock:
                self.services = services
                self.checked = now
                minute = int(self.clock() // 60) * 60
                self.buckets.setdefault(minute, {})["online"] = services["particle"].get("device_online")

    def snapshot(self):
        with self.lock:
            now = self.clock()
            minute = int(now // 60) * 60
            start = max(int(self.started // 60) * 60, minute - 3540)
            return {"started_at": self.started, "server_time": now,
                    "checked_at": self.checked, "services": dict(self.services),
                    "buckets": [{"time": t, **self.buckets.get(t, {})} for t in range(start, minute + 1, 60)],
                    "events": list(self.events)[-100:][::-1],
                    "cellular_usage_mb": None}

def parse_vitals(diag):
    """Use the diagnostic report's timestamp, never the browser refresh time."""
    import math
    def obj(v): return v if isinstance(v, dict) else {}
    def number(v, maximum=None):
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0:
            return None
        return v if maximum is None or v <= maximum else None
    stamp = diag.get("updated_at")
    try:
        parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        if parsed.tzinfo is None: return None
        timestamp = parsed.timestamp()
    except (ValueError, TypeError, AttributeError):
        return None
    device = obj(obj(diag.get("payload")).get("device"))
    signal = obj(obj(device.get("network")).get("signal"))
    memory = obj(obj(device.get("system")).get("memory"))
    used, total = number(memory.get("used")), number(memory.get("total"))
    rtt = number(obj(obj(device.get("cloud")).get("coap")).get("round_trip"))
    return {"time": timestamp, "strength": number(signal.get("strength"), 100),
            "quality": number(signal.get("quality"), 100),
            "rtt": None if rtt is None else rtt / 1000,
            "memory": used / total * 100 if used is not None and total and used <= total else None}

class Vitals:
    def __init__(self):
        self.lock = threading.Lock()
        self.samples = {}
    def add(self, diag):
        sample = parse_vitals(diag)
        if sample is None: return
        with self.lock:
            self.samples[sample["time"]] = sample
            self.samples = dict(sorted(self.samples.items())[-240:])
    def snapshot(self):
        with self.lock: return list(self.samples.values())
