import ast
from pathlib import Path
from types import SimpleNamespace
import unittest

class HTTPException(Exception):
    def __init__(self,status_code,detail):
        self.status_code=status_code; self.detail=detail

class RetryTests(unittest.TestCase):
    def setup_command(self, outcomes):
        node=next(n for n in ast.parse((Path(__file__).parents[1]/'main.py').read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='setmode')
        node.decorator_list=[]
        self.calls=[];self.events=[];self.state={}
        def once(x,b):
            self.calls.append(x.mode)
            outcome=outcomes.pop(0)
            if isinstance(outcome,Exception):raise outcome
            return {}
        self.env=dict(state=self.state,Mode=object,BackgroundTasks=object,HTTPException=HTTPException,setmode_once=once,
                      timeline=lambda *a,**k:self.events.append((a,k)),pub=lambda:dict(self.state),time=SimpleNamespace(sleep=lambda n:None))
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),self.env)
    def run_command(self):return self.env['setmode'](SimpleNamespace(mode='off'),None)
    def test_retries_same_disarm_and_clears_warning_on_success(self):
        self.setup_command([HTTPException(500,'temporary'),{}]);self.run_command()
        self.assertEqual(self.calls,['off','off']);self.assertIsNone(self.state['command_warning'])
        self.assertEqual(self.events[0][1]['kind'],'warning')
        self.assertIn('DISARM FAILED',self.events[0][0])
    def test_stops_after_three_and_keeps_warning(self):
        self.setup_command([HTTPException(500,'temporary') for _ in range(3)])
        with self.assertRaises(HTTPException):self.run_command()
        self.assertEqual(len(self.calls),3);self.assertIn('Not confirmed',self.state['command_warning'])
    def test_no_retry_auth_or_readiness_rejection(self):
        for code in (400,401,403,409,422):
            self.setup_command([HTTPException(code,'rejected')])
            with self.assertRaises(HTTPException):self.run_command()
            self.assertEqual(len(self.calls),1)
    def test_new_request_cancels_old_retry(self):
        self.setup_command([HTTPException(502,'temporary'),{}])
        self.env['time'].sleep=lambda n:self.state.update(_mode_request_id=object())
        with self.assertRaises(HTTPException):self.run_command()
        self.assertEqual(self.calls,['off'])
