import unittest
from monitoring import Monitor

class Reply:
    ok=True
    status_code=200
    def __init__(self, doc): self.doc=doc
    def json(self): return self.doc

class MonitoringTests(unittest.TestCase):
    def test_cache_and_safe_fields(self):
        now=[120]; m=Monitor(lambda: now[0]); calls=[]
        def get(url,**kwargs):
            calls.append(url)
            return Reply({"id":"device","connected":False,"secret":"hidden"} if "particle" in url else {"ok":True,"result":{"id":"private"}})
        for _ in range(3):m.check(get,"secret-token","device","bot-token",True)
        self.assertEqual(len(calls),2)
        data=m.snapshot()
        self.assertFalse(data["services"]["particle"]["device_online"])
        self.assertEqual(data["services"]["telegram"]["status"],"connected")
        self.assertNotIn("secret",str(data));self.assertNotIn("bot-token",str(data))
        now[0]+=60;m.check(get,"secret-token","device","bot-token",True)
        self.assertEqual(len(calls),4)
    def test_errors_never_reveal_token_or_claim_online(self):
        m=Monitor()
        def get(*a,**k):raise ValueError("https://secret-token")
        m.check(get,"secret","device","secret",True)
        data=m.snapshot()
        self.assertEqual(data["services"]["particle"]["status"],"unreachable")
        self.assertNotIn("secret",str(data))
    def test_missing_configuration_and_bounded_counters(self):
        now=[0];m=Monitor(lambda:now[0])
        m.check(lambda *a,**k:self.fail("Unexpected request"),"","","",False)
        self.assertEqual(m.snapshot()["services"]["telegram"]["status"],"not_configured")
        for i in range(600):
            now[0]=i*60;m.record("ping","failure")
        d=m.snapshot()
        self.assertEqual(len(d["events"]),100);self.assertEqual(len(d["buckets"]),60)
        self.assertEqual(d["buckets"][-1]["ping_failure"],1)
        self.assertIsNone(d["cellular_usage_mb"])

if __name__=="__main__":unittest.main()

class VitalsTests(unittest.TestCase):
    def test_units_missing_and_deduplication(self):
        from monitoring import parse_vitals, Vitals
        doc={"updated_at":"2026-10-03T12:00:00Z","payload":{"device":{"network":{"signal":{"strength":62,"quality":88}},"cloud":{"coap":{"round_trip":872}},"system":{"memory":{"used":75,"total":100}}}}}
        result=parse_vitals(doc)
        self.assertEqual(result["rtt"],.872)
        self.assertEqual(result["memory"],75)
        v=Vitals();v.add(doc);v.add(doc)
        self.assertEqual(len(v.snapshot()),1)
        self.assertIsNone(parse_vitals({}))
        doc["payload"]["device"]["network"]["signal"]["strength"]={"err":1}
        doc["payload"]["device"]["system"]["memory"]["total"]=0
        result=parse_vitals(doc)
        self.assertIsNone(result["strength"]);self.assertIsNone(result["memory"])
