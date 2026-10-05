"""Exercise Particle acknowledgement and queued configurations without network calls."""
import ast
import copy
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'main.py'

class ControlTests(unittest.TestCase):
    def setUp(self):
        tree=ast.parse(SOURCE.read_text())
        functions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('refresh_control_status','set_tracker_power_profile')]
        self.config={'sleep':{'mode':'enable'},'location':{'interval_min':3600,'interval_max':3600,'lock_trigger':True},'imu_trig':{'motion':'disable'},'geofence':{'zone1':{'enable':False}}}
        self.pending={}
        self.sent=[]
        self.online=True
        def put(*args,**kwargs):
            self.sent.append(kwargs['json'])
            return SimpleNamespace(ok=True)
        self.env={'state':{'mode':'off','tasking_state':'ready','_armed_motion_sensitivity':'high'},'time':SimpleNamespace(time=lambda:100),
                  'get_particle_config':lambda:{'configuration':{'current':copy.deepcopy(self.config),'pending':copy.deepcopy(self.pending)}},
                  'requests':SimpleNamespace(models=SimpleNamespace(complexjson=json),put=put,get=lambda *a,**k:SimpleNamespace(raise_for_status=lambda:None,json=lambda:{"connected":self.online})),
                  'PARTICLE_DEVICE_ID':'test','PARTICLE_ACCESS_TOKEN':'test','note':lambda *args:None,'timeline':lambda *a,**k:None,'deliver_config_now':lambda updated:False}
        exec(compile(ast.Module(body=functions,type_ignores=[]),str(SOURCE),'exec'),self.env)
    def refresh(self):
        self.env['refresh_control_status'](force=True)
        return self.env['state']['mode_status']
    def test_off_remains_reachable_with_hourly_reports(self):
        self.env['set_tracker_power_profile']('off')
        p=self.sent[-1]
        self.assertEqual(p['sleep']['mode'],'disable')
        self.assertEqual(p['location']['interval_max'],3600)
        self.assertFalse(p['location']['lock_trigger'])
        self.assertFalse(p['geofence']['zone1']['enable'])
        self.assertEqual(self.refresh(),'pending')
        self.config=p
        self.assertEqual(self.refresh(),'confirmed')
    def test_armed_motion_not_blocked_by_hourly_limit(self):
        self.env['state']['mode']='armed'
        self.env['set_tracker_power_profile']('armed')
        p=self.sent[-1]
        self.assertEqual(p['imu_trig']['motion'],'high')
        self.assertEqual(p['location']['interval_min'],60)
        self.assertEqual(p['location']['interval_max'],3600)
        self.pending=p
        self.assertEqual(self.refresh(),'pending')
        self.config=p;self.pending={}
        self.assertEqual(self.refresh(),'confirmed')
    def test_conflicting_pending_sleep_is_not_confirmed(self):
        self.config['sleep']['mode']='disable'
        self.pending={'sleep':{'mode':'enable'}}
        self.assertEqual(self.refresh(),'pending')
    def test_cloud_failure_never_confirms_armed(self):
        self.env['get_particle_config']=lambda:(_ for _ in ()).throw(RuntimeError('offline'))
        self.assertEqual(self.refresh(),'unknown')

    def test_storage_uses_hourly_sleep_and_never_ready(self):
        self.env['state']['tasking_state']='storage'
        self.env['set_tracker_power_profile']('off')
        self.config=self.sent[-1]
        self.assertEqual(self.config['sleep']['mode'],'enable')
        self.assertEqual(self.config['location']['interval_min'],3600)
        self.assertEqual(self.refresh(),'confirmed')
        self.assertEqual(self.env['state']['readiness'],'storage')
    def test_cloud_queue_is_not_device_ready(self):
        self.env['set_tracker_power_profile']('off','ready')
        self.pending=self.sent[-1]
        self.online=False
        self.refresh()
        self.assertEqual(self.env['state']['readiness'],'startup_pending')
        self.config=self.pending;self.pending={}
        self.refresh()
        self.assertEqual(self.env['state']['readiness'],'startup_pending')
        self.online=True
        self.refresh()
        self.assertEqual(self.env['state']['readiness'],'ready')
    def test_explicit_startup_overrides_storage(self):
        self.env['state']['tasking_state']='storage'
        self.env['set_tracker_power_profile']('off','ready')
        self.assertEqual(self.sent[-1]['sleep']['mode'],'disable')

if __name__=='__main__':unittest.main()

class DeliveryTests(unittest.TestCase):
    def test_online_delivery_and_offline_queue(self):
        tree=ast.parse(SOURCE.read_text())
        fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='deliver_config_now')
        sent=[]
        online=[True]
        def post(*a,**kw):
            sent.append(json.loads(kw['json']['arg']))
            return SimpleNamespace(raise_for_status=lambda:None,json=lambda:{'return_value':0})
        env={'requests':SimpleNamespace(models=SimpleNamespace(complexjson=json),post=post,
            get=lambda *a,**k:SimpleNamespace(raise_for_status=lambda:None,json=lambda:{'connected':online[0]})),
            'PARTICLE_ACCESS_TOKEN':'test','PARTICLE_DEVICE_ID':'test'}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),str(SOURCE),'exec'),env)
        profile={'imu_trig':{'motion':'high'},'sleep':{'mode':'disable'},'geofence':{'zone1':{}}}
        self.assertTrue(env['deliver_config_now'](profile))
        self.assertEqual(sent[0]['cfg'],{'imu_trig':{'motion':'high'},'sleep':{'mode':'disable'}})
        online[0]=False
        self.assertFalse(env['deliver_config_now'](profile))
        self.assertEqual(len(sent),1)
