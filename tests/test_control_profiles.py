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
        def put(*args,**kwargs):
            self.sent.append(kwargs['json'])
            return SimpleNamespace(ok=True)
        self.env={'state':{'mode':'off','_armed_motion_sensitivity':'high'},'time':SimpleNamespace(time=lambda:100),
                  'get_particle_config':lambda:{'configuration':{'current':copy.deepcopy(self.config),'pending':copy.deepcopy(self.pending)}},
                  'requests':SimpleNamespace(models=SimpleNamespace(complexjson=json),put=put),
                  'PARTICLE_DEVICE_ID':'test','PARTICLE_ACCESS_TOKEN':'test','note':lambda *args:None}
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

if __name__=='__main__':unittest.main()
