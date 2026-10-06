"""Full application routes with all external I/O blocked or simulated."""
import copy
from types import SimpleNamespace
import unittest
from unittest.mock import patch, Mock
from fastapi.testclient import TestClient
import main as app

INITIAL=copy.deepcopy(app.state)
class ModeFlowTests(unittest.TestCase):
    def setUp(self):
        app.state.clear();app.state.update(copy.deepcopy(INITIAL))
        self.current={'sleep':{'mode':'disable'},'imu_trig':{'motion':'disable'},'location':{'interval_min':3600,'interval_max':3600,'lock_trigger':False}}
        self.pending={};self.sent=[];self.sequence=[];self.apply=False;self.online=True
        def put(*a,**kw):
            self.assertEqual(app.state['mode_status'],'pending')
            self.sequence.append('config');self.sent.append(copy.deepcopy(kw['json']))
            self.pending=copy.deepcopy(kw['json'])
            if self.apply:self.current=copy.deepcopy(self.pending);self.pending={}
            return SimpleNamespace(ok=True)
        def location():
            self.assertEqual(app.state['mode_status'],'pending')
            self.sequence.append('location');return {'ok':True}
        self.location=Mock(side_effect=location)
        patches=[patch.object(app.requests.sessions.Session,'request',side_effect=AssertionError('Real network forbidden')),
                 patch.object(app,'get_particle_config',side_effect=lambda:{'configuration':{'current':self.current,'pending':self.pending}}),
                 patch.object(app.requests,'put',side_effect=put),
                 patch.object(app.requests,'get',side_effect=lambda *a,**k:SimpleNamespace(raise_for_status=lambda:None,json=lambda:{'connected':self.online})),
                 patch.object(app,'deliver_config_now',return_value=True),patch.object(app,'save_selected_mode'),
                 patch.object(app,'history_record'),patch.object(app,'send_alert'),patch.object(app,'request_fresh_location',self.location)]
        for p in patches:p.start();self.addCleanup(p.stop)
        self.client=TestClient(app.app) # No lifespan: no startup workers or external calls.
    def test_accepted_command_is_pending_for_both_modes(self):
        for mode in ('armed','geofence'):
            app.state['_armed_motion_sensitivity']='low'
            response=self.client.post('/api/trailer/mode',json={'mode':mode})
            self.assertEqual(response.status_code,200,response.text)
            self.assertEqual(response.json()['mode_status'],'pending')
            self.assertEqual(self.sent[-1]['imu_trig']['motion'],'high')
            self.assertEqual(self.sequence[-2:],['config','location'])
        self.assertEqual(self.location.call_count,2)
    def test_actual_profile_and_online_required(self):
        for mode in ('armed','geofence'):
            self.apply=True
            response=self.client.post('/api/trailer/mode',json={'mode':mode})
            self.assertEqual(response.json()['mode_status'],'confirmed')
            self.assertEqual(response.json()['last_confirmed_mode'],mode)
            self.online=False;app.refresh_control_status(force=True)
            self.assertNotEqual(app.state['mode_status'],'confirmed')
            self.online=True
    def test_confirmation_later_and_conflicting_pending(self):
        self.client.post('/api/trailer/mode',json={'mode':'armed'})
        self.current=copy.deepcopy(self.sent[-1]);self.pending={'imu_trig':{'motion':'disable'}}
        app.refresh_control_status(force=True);self.assertEqual(app.state['mode_status'],'pending')
        self.pending={};app.refresh_control_status(force=True)
        self.assertEqual(app.state['mode_status'],'confirmed')
        self.assertIsNone(app.state['pending_mode'])
    def test_refresh_cannot_confirm_during_submission(self):
        app.state.update(_mode_submitting=True,mode_status='pending')
        app.refresh_control_status(force=True)
        self.assertEqual(app.state['mode_status'],'pending')
    def test_off_clears_alarm_without_initial_ping(self):
        self.apply=True;app.state['alarm']=True
        r=self.client.post('/api/trailer/mode',json={'mode':'off'})
        self.assertEqual(r.json()['mode_status'],'confirmed')
        self.assertFalse(r.json()['alarm']);self.location.assert_not_called()
        self.assertEqual(self.sent[-1]['imu_trig']['motion'],'disable')
        self.assertEqual(self.sent[-1]['location']['interval_min'],3600)
    def test_first_motion_alert_and_clear(self):
        app.state.update(mode='armed',mode_status='confirmed')
        with patch.object(app,'TOKEN','test'):
            r=self.client.post('/api/particle/webhook',headers={'Authorization':'Bearer test'},json={'event':'imu_m','data':{}})
        self.assertEqual(r.status_code,200,r.text)
        self.assertTrue(app.state['alarm']);app.send_alert.assert_called_once()
        r=self.client.post('/api/trailer/clear-alarm')
        self.assertFalse(r.json()['alarm']);self.assertIsNone(app.state['_alarm_poll_next_at'])
    def test_off_motion_does_not_alarm(self):
        with patch.object(app,'TOKEN','test'):
            self.client.post('/api/particle/webhook',headers={'Authorization':'Bearer test'},json={'event':'imu_m','data':{}})
        self.assertFalse(app.state['alarm']);app.send_alert.assert_not_called()
    def test_single_worker_and_sixty_second_polling(self):
        worker=Mock();worker.is_alive.return_value=True
        with patch.object(app,'_alarm_poll_thread',worker),patch.object(app.threading,'Thread') as thread:
            app.ensure_alarm_poll_worker();thread.assert_not_called()
        app.state.update(alarm=True,_alarm_poll_next_at=0)
        stop=Mock();stop.is_set.side_effect=[False,True]
        with patch.object(app,'_alarm_poll_stop',stop),patch.object(app.time,'monotonic',return_value=100),patch.object(app,'request_fresh_location',return_value={'ok':True}) as ping:
            app.alarm_poll_worker();ping.assert_called_once()
        self.assertEqual(app.state['_alarm_poll_next_at'],160)
    def test_failed_command_keeps_last_confirmation_but_not_red(self):
        app.state['last_confirmed_mode']='off'
        with patch.object(app,'set_tracker_power_profile',side_effect=app.HTTPException(502,'test')),patch.object(app.time,'sleep'):
            r=self.client.post('/api/trailer/mode',json={'mode':'armed'})
        self.assertEqual(r.status_code,502)
        self.assertEqual(app.state['last_confirmed_mode'],'off')
        self.assertNotEqual(app.state['mode_status'],'confirmed')
        self.assertIn('FAILED',app.state['command_warning'])
    def test_old_selector_rejected_without_config_write(self):
        self.assertEqual(self.client.post('/api/trailer/imu-sensitivity',json={'sensitivity':'low'}).status_code,409)
        self.assertEqual(self.sent,[])
    def test_manual_ping_and_resync_preserved(self):
        with patch.object(app,'request_fresh_location',return_value={'ok':True}) as ping:
            self.assertEqual(self.client.post('/api/trailer/ping').status_code,200)
            ping.assert_called_once()
        app.state.update(command_warning='old failure',_mode_failed=True)
        with patch.object(app,'refresh_vitals'):
            r=self.client.post('/api/trailer/resync')
        self.assertEqual(r.status_code,200)
        self.assertIsNone(r.json()['command_warning'])
        self.assertEqual(r.json()['mode_status'],'confirmed')
    def test_geofence_motion_inside_no_alarm_outside_alarms(self):
        app.state.update(mode='geofence',mode_status='confirmed',home_lat=40.0,home_lon=-75.0)
        with patch.object(app,'TOKEN','test'):
            r=self.client.post('/api/particle/webhook',headers={'Authorization':'Bearer test'},json={'lat':40.0,'lon':-75.0,'motion':True})
            self.assertEqual(r.status_code,200)
            self.assertFalse(app.state['alarm'])
            r=self.client.post('/api/particle/webhook',headers={'Authorization':'Bearer test'},json={'lat':41.0,'lon':-75.0,'motion':True})
            self.assertEqual(r.status_code,200)
            self.assertTrue(app.state['alarm'])
    def test_clear_stops_next_worker_ping(self):
        app.state.update(mode='armed',alarm=True)
        self.client.post('/api/trailer/clear-alarm')
        stop=Mock();stop.is_set.side_effect=[False,True]
        with patch.object(app,'_alarm_poll_stop',stop),patch.object(app,'_alarm_poll_wake'),patch.object(app,'request_fresh_location') as ping:
            app.alarm_poll_worker();ping.assert_not_called()
