"""Replay paired Particle reports without any tracker, database or Telegram I/O."""
import copy
import json
import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient
import main as app

INITIAL=copy.deepcopy(app.state)

class LocationDeliveryTests(unittest.TestCase):
    def setUp(self):
        app.state.clear();app.state.update(copy.deepcopy(INITIAL))
        app._location_deliveries.clear()
        patches=[patch.object(app,'TOKEN','test'),patch.object(app,'history_record'),
                 patch.object(app,'send_alert'),patch.object(app,'ensure_alarm_poll_worker'),
                 patch.object(app.requests.sessions.Session,'request',side_effect=AssertionError('Real network forbidden'))]
        for p in patches:
            p.start();self.addCleanup(p.stop)
        self.addCleanup(app._location_deliveries.clear)
        self.client=TestClient(app.app)

    def report(self,time=1791751625,lat=36.61021783,lon=-76.03202433,trig=None,device='tracker-one',published='2026-10-11T20:47:13.864Z',string_data=False):
        data={'time':time,'loc':{'lat':lat,'lon':lon,'lck':1,'spd':0.05},'batt':97,'trig':trig or ['user']}
        body={'event':'loc','coreid':device,'published_at':published,'data':json.dumps(data) if string_data else data}
        return self.client.post('/api/particle/webhook',headers={'Authorization':'Bearer test'},json=body)

    def location_writes(self):
        return [c for c in app.history_record.call_args_list if c.args[:2]==('location','LOCATION UPDATE')]

    def test_paired_publish_times_produce_one_full_row_and_one_timeline_step(self):
        self.assertEqual(self.report().status_code,200)
        duplicate=self.report(published='2026-10-11T20:47:14.128Z',string_data=True)
        self.assertTrue(duplicate.json()['duplicate'])
        self.assertEqual(len(app.state['route']),1)
        self.assertEqual(len(app.state['timeline']),1)
        self.assertEqual(app.state['timeline'][0]['stage'],'LOCATION UPDATE')
        writes=self.location_writes();self.assertEqual(len(writes),1)
        self.assertEqual(writes[0].kwargs['lat'],36.61021783)
        self.assertEqual(writes[0].kwargs['lon'],-76.03202433)
        self.assertEqual(app.history_record.call_count,1)

    def test_empty_first_delivery_does_not_block_full_report(self):
        empty=self.report(lat=None,lon=None)
        self.assertIn('ignored',empty.json())
        self.assertNotIn('duplicate',self.report().json())
        self.assertEqual(len(self.location_writes()),1)

    def test_later_fix_at_exact_same_position_is_kept(self):
        self.report();later=self.report(time=1791751685)
        self.assertNotIn('duplicate',later.json())
        self.assertEqual(len(self.location_writes()),2)

    def test_return_to_previous_position_is_kept(self):
        self.report();self.report(time=1791751685,lat=36.611)
        self.report(time=1791751745)
        self.assertEqual(len(app.state['route']),3)
        self.assertEqual(app.state['route'][0],app.state['route'][2])

    def test_changed_motion_payload_is_kept_and_paired_motion_alerts_once(self):
        app.state['mode']='armed'
        self.report()
        self.report(trig=['imu_m','user'])
        duplicate=self.report(trig=['imu_m','user'],published='2026-10-11T20:47:14.128Z')
        self.assertTrue(duplicate.json()['duplicate'])
        self.assertTrue(app.state['alarm'])
        app.send_alert.assert_called_once()
        self.assertEqual(len(self.location_writes()),2)

    def test_distinct_device_reports_are_kept(self):
        self.report();self.report(device='tracker-two')
        self.assertEqual(len(self.location_writes()),2)

    def test_untimed_and_changed_fixes_are_not_discarded(self):
        payload={'lat':36.61,'lon':-76.03}
        for _ in range(2):
            self.client.post('/api/particle/webhook',headers={'Authorization':'Bearer test'},json=payload)
        self.report();self.report(lat=36.611)
        self.assertEqual(len(self.location_writes()),4)

    def test_duplicate_window_expires(self):
        with patch.object(app.time,'monotonic',return_value=100):self.report()
        with patch.object(app.time,'monotonic',return_value=701):self.report()
        self.assertEqual(len(self.location_writes()),2)

if __name__=='__main__':unittest.main()
