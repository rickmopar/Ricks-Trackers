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
