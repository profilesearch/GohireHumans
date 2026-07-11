import contextlib
import hashlib
import importlib.util
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

import stripe as stripe_sdk
from test_deep_audit_regressions import load_api_core, parse_cgi_output


class RefundDisputeLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ['DATABASE_PATH'] = os.path.join(self.tmp.name, 'refund.db')
        os.environ['DISABLE_AUTO_SEED'] = '1'
        self.api = load_api_core(); self.api._db_path_resolved = None; self.api.init_db()
        self.api.PRODUCTION_MODE = True; self.api.STRIPE_AVAILABLE = True; self.api.STRIPE_SECRET_KEY = 'test'
        self.create = mock.Mock(side_effect=self._create_refund)
        self.retrieve = mock.Mock()
        self.list = mock.Mock(return_value={'data': []})
        self.api.stripe = SimpleNamespace(
            Refund=SimpleNamespace(create=self.create, retrieve=self.retrieve, list=self.list),
            Webhook=SimpleNamespace(construct_event=mock.Mock()),
            StripeError=stripe_sdk.StripeError, APIConnectionError=stripe_sdk.APIConnectionError,
            InvalidRequestError=stripe_sdk.InvalidRequestError)
        self.api.STRIPE_ERROR = stripe_sdk.StripeError
        with self.api.get_db() as db:
            db.execute("INSERT INTO users(id,email,name,password_hash,is_admin) VALUES(1,'admin@x','Admin',?,1)", [self.api.hash_password('correct horse')])
            db.execute("INSERT INTO users(id,email,name,password_hash,is_admin) VALUES(4,'admin2@x','Admin Two','x',1)")
            db.execute("INSERT INTO users(id,email,name,password_hash) VALUES(2,'worker@x','Worker','x')")
            db.execute("INSERT INTO users(id,email,name,password_hash) VALUES(3,'buyer@x','Buyer','x')")
            for uid,tok in [(1,'admin'),(2,'worker'),(3,'buyer')]: db.execute("INSERT INTO sessions(user_id,token,expires_at) VALUES(?,?,datetime('now','+1 day'))",[uid,tok])
            db.execute("INSERT INTO orders(id,type,worker_id,employer_id,status,total_amount) VALUES(10,'service_order',2,3,'submitted',10)")
            charge=self.api.buyer_charge_breakdown_cents(10); fp=self.api.funding_request_fingerprint('order:10',3,10,None,charge)
            aid=db.execute("""INSERT INTO funding_attempts(operation_key,attempt_number,request_fingerprint,processor_idempotency_key,employer_id,order_id,base_amount_cents,platform_fee_cents,processing_fee_cents,charged_total_cents,currency,status,stripe_payment_intent_id,processor_status,evidence_source,processor_evidence_at,committed_at) VALUES('order:10',1,?,'fund:10',3,10,1000,10,30,1040,'usd','committed','pi_10','succeeded','processor_create',datetime('now'),datetime('now'))""",[fp]).lastrowid
            db.execute("INSERT INTO escrow_holds(order_id,amount,base_amount_cents,platform_fee_cents,processing_fee_cents,charged_total_cents,fee_policy_version,funding_identity,funding_attempt_id,status,stripe_payment_intent_id) VALUES(10,10,1000,10,30,1040,'component-half-up-v1','order:10',?,'held','pi_10')",[aid]); db.commit()
        self.request('POST','/orders/10/dispute','buyer',{'reason':'not delivered'})

    def tearDown(self):
        self.tmp.cleanup(); os.environ.pop('DATABASE_PATH',None); os.environ.pop('DISABLE_AUTO_SEED',None)

    def _create_refund(self, **kw):
        evidence={'id':'re_10','payment_intent':kw['payment_intent'],'amount':kw['amount'],'currency':'usd','metadata':kw['metadata'],'status':'succeeded'}
        self.retrieve.return_value=evidence
        return evidence

    def request(self, method, path, token='', payload=None):
        for x in ('body_cache','raw_body'):
            if hasattr(self.api._request_ctx,x): delattr(self.api._request_ctx,x)
        raw=json.dumps(payload or {}); c=self.api._request_ctx
        c.request_method=method;c.path_info=path;c.query_string='';c.http_authorization=f'Bearer {token}' if token else '';c.http_x_api_key='';c.stdin_data=raw;c.content_type='application/json';c.content_length=str(len(raw));c.remote_addr='127.0.0.1';c.http_stripe_signature='sig'
        with contextlib.redirect_stdout(io.StringIO()) as out:self.api.handle_request()
        return parse_cgi_output(out.getvalue())

    def test_open_dispute_is_durable(self):
        with self.api.get_db() as db:
            row=db.execute('SELECT * FROM disputes WHERE order_id=10').fetchone()
            self.assertEqual(row['opened_by'],3); self.assertEqual(row['status'],'open')

    def test_exact_refund_settles_atomically_and_replays(self):
        status, body=self.request('POST','/admin/resolve-dispute','admin',{'order_id':10,'resolution':'refund_to_employer','admin_password':'correct horse'})
        self.assertEqual(status,200,body)
        status2, body2=self.request('POST','/admin/resolve-dispute','admin',{'order_id':10,'resolution':'refund_to_employer','admin_password':'correct horse'})
        self.assertEqual(status2,200,body2); self.assertEqual(self.create.call_count,1)
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT status FROM orders WHERE id=10').fetchone()[0],'canceled')
            self.assertEqual(db.execute('SELECT status FROM escrow_holds WHERE order_id=10').fetchone()[0],'refunded')
            a=db.execute('SELECT * FROM refund_attempts').fetchone(); self.assertEqual((a['status'],a['lifecycle_status'],a['amount_cents']),('committed','completed',1000))

    def test_pending_does_not_settle_and_release_split_are_closed(self):
        self.create.side_effect=lambda **kw:{'id':'re_pending','payment_intent':'pi_10','amount':1000,'currency':'usd','metadata':kw['metadata'],'status':'pending'}
        status,_=self.request('POST','/admin/resolve-dispute','admin',{'order_id':10,'resolution':'refund_to_employer','admin_password':'correct horse'})
        self.assertEqual(status,202)
        for resolution in ('release_to_worker','split'):
            self.assertEqual(self.request('POST','/admin/resolve-dispute','admin',{'order_id':10,'resolution':resolution,'admin_password':'correct horse'})[0],503)
        with self.api.get_db() as db:self.assertEqual(db.execute('SELECT status FROM orders WHERE id=10').fetchone()[0],'disputed')

    def test_mismatched_evidence_freezes_manual_review(self):
        self.create.side_effect=lambda **kw:{'id':'re_bad','payment_intent':'pi_other','amount':1000,'currency':'usd','metadata':kw['metadata'],'status':'succeeded'}
        status,_=self.request('POST','/admin/resolve-dispute','admin',{'order_id':10,'resolution':'refund_to_employer','admin_password':'correct horse'})
        self.assertEqual(status,409)
        with self.api.get_db() as db:self.assertEqual(db.execute('SELECT manual_review_required FROM refund_attempts').fetchone()[0],1)

    def test_schema_poisoning_rolls_back_startup(self):
        with self.api.get_db() as db: db.execute('DROP INDEX idx_refund_attempts_processor_key'); db.commit()
        with self.assertRaisesRegex(RuntimeError,'refund schema'):
            self.api.init_db()
        with self.api.get_db() as db:self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_refund_attempts_processor_key'").fetchone())
    def _resolve(self):
        return self.request('POST','/admin/resolve-dispute','admin',{
            'order_id':10,'resolution':'refund_to_employer','admin_password':'correct horse'
        })

    def _attempt(self):
        with self.api.get_db() as db:
            return db.execute('SELECT * FROM refund_attempts ORDER BY id DESC LIMIT 1').fetchone()

    def _exact_evidence(self, attempt, status='succeeded', **changes):
        evidence={
            'id':attempt['processor_refund_id'] or 're_recovered',
            'payment_intent':attempt['payment_intent_id'],
            'amount':attempt['amount_cents'],'currency':attempt['currency'],
            'metadata':self.api._refund_metadata(attempt),'status':status,
        }
        evidence.update(changes)
        return evidence

    def _crash_child(self, after_call_start=False):
        marker=os.path.join(self.tmp.name,'processor-call.marker')
        backend=os.path.dirname(__file__)
        if after_call_start:
            action=f"""
class Refund:
    @staticmethod
    def list(**kw): return {{'data':[]}}
    @staticmethod
    def create(**kw):
        with open({marker!r},'a',encoding='utf-8') as fh:
            fh.write('called\\n'); fh.flush(); os.fsync(fh.fileno())
        os._exit(74)
api.stripe=SimpleNamespace(Refund=Refund)
"""
            expected=74
        else:
            action="api._try_list_refund_candidates=lambda attempt: os._exit(73)"
            expected=73
        script=f"""
import os,sys
from types import SimpleNamespace
sys.path.insert(0,{backend!r})
import api_core as api
api._db_path_resolved=os.environ['DATABASE_PATH']
{action}
db=api.get_db()
api.issue_dispute_refund(db,10,1)
"""
        proc=subprocess.run([sys.executable,'-c',script],env=os.environ.copy(),capture_output=True,text=True,timeout=30)
        self.assertEqual(proc.returncode,expected,proc.stderr)
        return marker

    def _prepare_processor_succeeded(self):
        self.create.side_effect=lambda **kw:{'id':'re_pending','payment_intent':'pi_10','amount':1000,'currency':'usd','metadata':kw['metadata'],'status':'pending'}
        self.assertEqual(self._resolve()[0],202)
        attempt=self._attempt(); evidence=self._exact_evidence(attempt,id='re_pending')
        with self.api.get_db() as db:
            result=self.api.reconcile_refund_attempt(db,attempt,apply=False,evidence=evidence,evidence_source='processor_retrieve')
        self.assertEqual(result['outcome'],'succeeded')
        return attempt['id']

    def _assert_unsettled(self, attempt_id):
        with self.api.get_db() as db:
            self.assertEqual(db.execute('SELECT status FROM orders WHERE id=10').fetchone()[0],'disputed')
            self.assertEqual(db.execute('SELECT status FROM escrow_holds WHERE order_id=10').fetchone()[0],'held')
            self.assertEqual(db.execute('SELECT status FROM disputes WHERE order_id=10').fetchone()[0],'open')
            self.assertEqual(db.execute('SELECT status FROM refund_attempts WHERE id=?',[attempt_id]).fetchone()[0],'processor_succeeded')
            self.assertEqual(db.execute("SELECT COUNT(*) FROM audit_log WHERE action='resolve_dispute_refund'").fetchone()[0],0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM notifications WHERE type LIKE 'refund_committed:%'").fetchone()[0],0)

    def test_real_restart_after_prepared_commit_resumes_same_attempt(self):
        self._crash_child(after_call_start=False)
        with self.api.get_db() as db:
            row=db.execute('SELECT * FROM refund_attempts').fetchone()
            self.assertEqual(row['status'],'prepared'); self.assertIsNone(row['processor_call_started_at'])
        status,body=self._resolve()
        self.assertEqual(status,200,body); self.assertEqual(self.create.call_count,1)
        with self.api.get_db() as db:
            rows=db.execute('SELECT attempt_number,status FROM refund_attempts').fetchall()
            self.assertEqual([(r[0],r[1]) for r in rows],[(1,'committed')])

    def test_real_restart_after_call_start_never_creates_again_without_evidence(self):
        marker=self._crash_child(after_call_start=True)
        with open(marker,encoding='utf-8') as fh:self.assertEqual(fh.read().splitlines(),['called'])
        with self.api.get_db() as db:
            row=db.execute('SELECT * FROM refund_attempts').fetchone()
            self.assertEqual(row['status'],'prepared'); self.assertIsNotNone(row['processor_call_started_at'])
        status,body=self._resolve()
        self.assertEqual(status,202,body); self.assertEqual(self.create.call_count,0)

    def test_real_restart_recovers_exact_processor_search_without_create(self):
        self._crash_child(after_call_start=True)
        attempt=self._attempt(); self.list.return_value={'data':[self._exact_evidence(attempt)]}
        status,body=self._resolve()
        self.assertEqual(status,200,body); self.assertEqual(self.create.call_count,0)

    def test_pending_then_succeeded_retrieval_settles_once(self):
        self.create.side_effect=lambda **kw:{'id':'re_pending','payment_intent':'pi_10','amount':1000,'currency':'usd','metadata':kw['metadata'],'status':'requires_action'}
        self.assertEqual(self._resolve()[0],202)
        attempt=self._attempt(); self.retrieve.return_value=self._exact_evidence(attempt,id='re_pending')
        status,body=self._resolve()
        self.assertEqual(status,200,body); self.assertEqual(self.create.call_count,1)

    def test_definitive_preoperation_failure_allows_numbered_retry(self):
        failure=stripe_sdk.InvalidRequestError('rejected before operation','amount'); calls=[]
        def fail_then_succeed(**kw):
            calls.append(1)
            if len(calls)==1:raise failure
            return self._create_refund(**kw)
        self.create.side_effect=fail_then_succeed
        self.assertEqual(self._resolve()[0],409)
        status,body=self._resolve()
        self.assertEqual(status,200,body)
        with self.api.get_db() as db:
            rows=db.execute('SELECT attempt_number,status,lifecycle_status FROM refund_attempts ORDER BY attempt_number').fetchall()
            self.assertEqual([tuple(r) for r in rows],[(1,'failed','completed'),(2,'committed','completed')])

    def test_sparse_create_is_retrieved_before_settlement(self):
        metadata={}
        def sparse(**kw):
            metadata.update(kw['metadata']); return {'id':'re_sparse'}
        self.create.side_effect=sparse
        self.retrieve.side_effect=lambda rid:{'id':rid,'payment_intent':'pi_10','amount':1000,'currency':'usd','metadata':dict(metadata),'status':'succeeded'}
        status,body=self._resolve()
        self.assertEqual(status,200,body); self.assertEqual(self.retrieve.call_count,1)

    def test_duplicate_search_evidence_freezes_without_create(self):
        def duplicates(**kw):
            attempt=self._attempt(); evidence=self._exact_evidence(attempt)
            return {'data':[dict(evidence,id='re_a'),dict(evidence,id='re_b')]}
        self.list.side_effect=duplicates
        status,body=self._resolve()
        self.assertEqual(status,409,body); self.assertEqual(self.create.call_count,0)
        with self.api.get_db() as db:self.assertEqual(db.execute('SELECT manual_review_required FROM refund_attempts').fetchone()[0],1)

    def test_processor_refund_id_owner_conflict_freezes_both_without_unique_rollback(self):
        self._crash_child(after_call_start=False); subject=self._attempt()
        with self.api.get_db() as db:
            db.execute("INSERT INTO services(id,worker_id,title,description,category,pricing_type,price,status) VALUES(11,2,'Owner fixture','Owner fixture','Testing','fixed',10,'active')")
            db.execute("INSERT INTO orders(id,type,service_id,worker_id,employer_id,status,total_amount) VALUES(11,'service_order',11,2,3,'disputed',10)")
            charge=self.api.buyer_charge_breakdown_cents(10);owner_fp=self.api.funding_request_fingerprint('order:11',3,10,None,charge)
            cur=db.execute("""INSERT INTO funding_attempts(operation_key,attempt_number,request_fingerprint,processor_idempotency_key,employer_id,order_id,base_amount_cents,platform_fee_cents,processing_fee_cents,charged_total_cents,currency,status,stripe_payment_intent_id,processor_status,evidence_source,processor_evidence_at,committed_at) VALUES('order:11',1,?,'fund-owner',3,11,1000,10,30,1040,'usd','committed','pi_11','succeeded','processor_create',datetime('now'),datetime('now'))""",[owner_fp])
            funding_id=cur.lastrowid
            hold_id=db.execute("INSERT INTO escrow_holds(order_id,amount,base_amount_cents,platform_fee_cents,processing_fee_cents,charged_total_cents,fee_policy_version,funding_identity,funding_attempt_id,status,stripe_payment_intent_id) VALUES(11,10,1000,10,30,1040,'component-half-up-v1','order:11',?,'held','pi_11')",[funding_id]).lastrowid
            dispute_id=db.execute("INSERT INTO disputes(order_id,opened_by,reason,reason_sha256,reason_length,source,status) VALUES(11,3,'owner case',?,10,'participant','open')",[hashlib.sha256(b'owner case').hexdigest()]).lastrowid
            hold=db.execute('SELECT * FROM escrow_holds WHERE id=?',[hold_id]).fetchone();dispute=db.execute('SELECT * FROM disputes WHERE id=?',[dispute_id]).fetchone();order=db.execute('SELECT * FROM orders WHERE id=11').fetchone();funding=db.execute('SELECT * FROM funding_attempts WHERE id=?',[funding_id]).fetchone()
            hs,hh,ls,lh=self.api._refund_snapshots(db,hold,dispute,order,funding)
            owner_id=db.execute("""INSERT INTO refund_attempts(operation_key,attempt_number,request_fingerprint,dispute_id,order_id,hold_id,funding_attempt_id,admin_id,employer_id,worker_id,amount_cents,currency,payment_intent_id,processor_idempotency_key,status,processor_refund_id,expected_hold_snapshot_json,expected_hold_snapshot_sha256,expected_lifecycle_snapshot_json,expected_lifecycle_snapshot_sha256) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",[f'refund:hold:{hold_id}',1,'f'*64,dispute_id,11,hold_id,funding_id,1,3,2,1000,'usd','pi_11','k'*64,'prepared','re_owned',hs,hh,ls,lh]).lastrowid
            db.commit()
            result=self.api._record_refund_evidence(db,subject,self._exact_evidence(subject,id='re_owned'),'processor_search')
            self.assertEqual(result,'manual_review')
            rows=db.execute('SELECT id,lifecycle_status,manual_review_required FROM refund_attempts ORDER BY id').fetchall()
            self.assertEqual([(r['id'],r['lifecycle_status'],r['manual_review_required']) for r in rows],[(subject['id'],'manual_review',1),(owner_id,'manual_review',1)])
            evidence=[r[0] for r in db.execute('SELECT redacted_evidence_json FROM refund_attempt_conflict_evidence ORDER BY id')]
            self.assertEqual(len(evidence),2);self.assertTrue(all('re_owned' not in item for item in evidence))
            db.execute("UPDATE refund_attempts SET lifecycle_status='pending',manual_review_required=0,error_code=NULL");db.commit()
            owner=db.execute('SELECT * FROM refund_attempts WHERE id=?',[owner_id]).fetchone()
            result=self.api._record_refund_evidence(db,owner,self._exact_evidence(subject,id='re_owned'),'signed_webhook')
            self.assertEqual(result,'manual_review')
            self.assertEqual(db.execute("SELECT COUNT(*) FROM refund_attempts WHERE lifecycle_status='manual_review' AND manual_review_required=1").fetchone()[0],2)
            evidence=[r[0] for r in db.execute('SELECT redacted_evidence_json FROM refund_attempt_conflict_evidence ORDER BY id')]
            self.assertEqual(len(evidence),4);self.assertTrue(all('re_owned' not in item for item in evidence))

    def test_malformed_processor_metadata_freezes_durably_without_settlement(self):
        self._crash_child(after_call_start=False)
        attempt=self._attempt()
        malformed=self._exact_evidence(attempt)
        malformed['metadata']='attempt_id=1'
        with self.api.get_db() as db:
            result=self.api.reconcile_refund_attempt(
                db,attempt,apply=True,evidence=malformed,evidence_source='processor_retrieve'
            )
            self.assertEqual(result['outcome'],'manual_review')
            row=db.execute(
                'SELECT status,lifecycle_status,manual_review_required,error_code FROM refund_attempts WHERE id=?',
                [attempt['id']],
            ).fetchone()
            self.assertEqual(tuple(row),('prepared','manual_review',1,'malformed_processor_metadata'))
            conflict=db.execute(
                'SELECT conflict_type,redacted_evidence_json FROM refund_attempt_conflict_evidence WHERE attempt_id=?',
                [attempt['id']],
            ).fetchone()
            self.assertEqual(conflict['conflict_type'],'malformed_processor_metadata')
            self.assertNotIn('attempt_id=1',conflict['redacted_evidence_json'])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM audit_log WHERE action='resolve_dispute_refund'").fetchone()[0],0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM notifications WHERE type LIKE 'refund_committed:%'").fetchone()[0],0)

    def test_committed_replay_retrieves_and_freezes_contradiction(self):
        self.assertEqual(self._resolve()[0],200)
        attempt=self._attempt(); self.retrieve.return_value=self._exact_evidence(attempt,amount=999)
        status,body=self._resolve()
        self.assertEqual(status,409,body)
        with self.api.get_db() as db:
            row=db.execute('SELECT status,lifecycle_status,manual_review_required FROM refund_attempts').fetchone()
            self.assertEqual(tuple(row),('committed','manual_review',1))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM audit_log WHERE action='resolve_dispute_refund'").fetchone()[0],1)

    def test_transient_committed_retrieve_is_unresolved_without_corruption(self):
        self.assertEqual(self._resolve()[0],200)
        self.retrieve.side_effect=stripe_sdk.APIConnectionError('temporary outage')
        status,body=self._resolve()
        self.assertEqual(status,202,body)
        with self.api.get_db() as db:
            row=db.execute('SELECT status,lifecycle_status,manual_review_required FROM refund_attempts').fetchone()
            self.assertEqual(tuple(row),('committed','completed',0))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM refund_attempt_conflict_evidence').fetchone()[0],0)

    def test_settlement_rolls_back_when_audit_fails(self):
        attempt_id=self._prepare_processor_succeeded()
        db=self.api.get_db()
        try:
            with mock.patch.object(self.api,'audit',side_effect=RuntimeError('audit failed')):
                with self.assertRaisesRegex(RuntimeError,'audit failed'):self.api._settle_refund_attempt(db,attempt_id)
        finally:db.close()
        self._assert_unsettled(attempt_id)
        with self.api.get_db() as db:self.assertEqual(self.api._settle_refund_attempt(db,attempt_id)['outcome'],'succeeded')

    def test_settlement_rolls_back_when_second_notification_fails(self):
        attempt_id=self._prepare_processor_succeeded(); original=self.api.push_notification; calls=[]
        def fail_second(*args,**kwargs):
            calls.append(args[1])
            if len(calls)==2:raise RuntimeError('second notification failed')
            return original(*args,**kwargs)
        db=self.api.get_db()
        try:
            with mock.patch.object(self.api,'push_notification',side_effect=fail_second):
                with self.assertRaisesRegex(RuntimeError,'second notification failed'):self.api._settle_refund_attempt(db,attempt_id)
        finally:db.close()
        self._assert_unsettled(attempt_id)

    def test_settlement_rolls_back_when_first_notification_fails(self):
        attempt_id=self._prepare_processor_succeeded(); db=self.api.get_db()
        try:
            with mock.patch.object(self.api,'push_notification',side_effect=RuntimeError('first notification failed')):
                with self.assertRaisesRegex(RuntimeError,'first notification failed'):self.api._settle_refund_attempt(db,attempt_id)
        finally:db.close()
        self._assert_unsettled(attempt_id)

    def test_hourly_contract_refund_remains_disabled_before_processor(self):
        with self.api.get_db() as db:
            db.execute("INSERT INTO hourly_contracts(order_id,hourly_rate,weekly_hour_cap,status) VALUES(10,25,40,'active')");db.commit()
        status,body=self._resolve()
        self.assertEqual(status,409,body); self.assertEqual(self.create.call_count,0)
        with self.api.get_db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM refund_attempts').fetchone()[0],0)

    def test_concurrent_admin_calls_cross_processor_once(self):
        entered=threading.Event(); release=threading.Event(); results=[]; errors=[]
        def blocking_create(**kw):
            entered.set()
            if not release.wait(10):raise RuntimeError('test release timeout')
            return self._create_refund(**kw)
        self.create.side_effect=blocking_create
        def run():
            db=self.api.get_db()
            try:results.append(self.api.issue_dispute_refund(db,10,1))
            except Exception as exc:errors.append(exc)
            finally:db.close()
        first=threading.Thread(target=run); first.start(); self.assertTrue(entered.wait(10))
        second=threading.Thread(target=run); second.start(); second.join(10)
        self.assertFalse(second.is_alive()); release.set(); first.join(10)
        self.assertFalse(first.is_alive()); self.assertFalse(errors,errors); self.assertEqual(self.create.call_count,1)
        self.assertEqual(sorted(r['outcome'] for r in results),['succeeded','unresolved'])

    def test_duplicate_signed_evidence_preserves_exactly_once_effects(self):
        self.assertEqual(self._resolve()[0],200); attempt=self._attempt()
        with self.api.get_db() as db:
            before=db.execute('SELECT updated_at,committed_at FROM refund_attempts WHERE id=?',[attempt['id']]).fetchone()
            result=self.api.reconcile_refund_attempt(db,attempt,True,self._exact_evidence(attempt),'signed_webhook')
            after=db.execute('SELECT updated_at,committed_at FROM refund_attempts WHERE id=?',[attempt['id']]).fetchone()
            self.assertEqual(result['outcome'],'succeeded'); self.assertEqual(tuple(before),tuple(after))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM audit_log WHERE action='resolve_dispute_refund'").fetchone()[0],1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM notifications WHERE type=?",[f'refund_committed:{attempt["id"]}']).fetchone()[0],2)

    def test_dispute_reason_is_private_immutable_and_all_admins_are_notified(self):
        reason='not delivered'
        with self.api.get_db() as db:
            dispute=db.execute('SELECT * FROM disputes WHERE order_id=10').fetchone()
            self.assertEqual(dispute['reason'],reason); self.assertEqual(dispute['reason_length'],len(reason))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM notifications WHERE type='admin_dispute'").fetchone()[0],2)
            notification_blob=json.dumps([dict(r) for r in db.execute('SELECT * FROM notifications')])
            audit_blob=json.dumps([dict(r) for r in db.execute("SELECT * FROM audit_log WHERE action='dispute_order'")])
            self.assertNotIn(reason,notification_blob); self.assertNotIn(reason,audit_blob)
            with self.assertRaises(sqlite3.IntegrityError):db.execute("UPDATE disputes SET reason='changed reason',reason_length=14 WHERE order_id=10")
            db.rollback()
            with self.assertRaises(sqlite3.IntegrityError):db.execute('DELETE FROM disputes WHERE order_id=10')
            db.rollback()

    def test_unexpected_refund_trigger_blocks_startup_before_repair(self):
        with self.api.get_db() as db:
            db.execute("CREATE TRIGGER poisoned_refund_trigger BEFORE UPDATE ON disputes BEGIN SELECT 1; END");db.commit()
        with self.assertRaisesRegex(RuntimeError,'unexpected protected trigger|refund schema'):self.api.init_db()
        with self.api.get_db() as db:self.assertIsNotNone(db.execute("SELECT name FROM sqlite_master WHERE name='poisoned_refund_trigger'").fetchone())

    def test_reconciliation_tool_paginates_exactly_and_redacts_identifiers(self):
        self._crash_child(after_call_start=False); attempt=self._attempt()
        exact=self._exact_evidence(attempt,id='re_sensitive_identifier')
        wrong=dict(exact,id='re_wrong_identifier',amount=999)
        calls=[]
        class RefundApi:
            @staticmethod
            def list(**kwargs):
                calls.append(dict(kwargs))
                if len(calls)==1:return {'data':[wrong],'has_more':True}
                return {'data':[exact],'has_more':False}
            @staticmethod
            def retrieve(refund_id):raise AssertionError('retrieve must not run')
        tool_path=os.path.join(os.path.dirname(__file__),'tools','reconcile_refund_attempts.py')
        spec=importlib.util.spec_from_file_location('refund_reconcile_tool',tool_path)
        if spec is None or spec.loader is None:self.fail('could not load reconciliation tool')
        tool=importlib.util.module_from_spec(spec);spec.loader.exec_module(tool)
        with self.api.get_db() as db:
            before=tuple(db.execute('SELECT status,processor_call_started_at FROM refund_attempts WHERE id=?',[attempt['id']]).fetchone())
            report=tool.build_report(db,RefundApi,attempt_id=attempt['id'])
            after=tuple(db.execute('SELECT status,processor_call_started_at FROM refund_attempts WHERE id=?',[attempt['id']]).fetchone())
        self.assertEqual(before,after);self.assertEqual(len(calls),2)
        self.assertEqual(calls[1]['starting_after'],'re_wrong_identifier')
        self.assertTrue(report['attempts'][0]['exact_candidate'])
        blob=json.dumps(report)
        self.assertNotIn('re_sensitive_identifier',blob);self.assertNotIn('re_wrong_identifier',blob);self.assertNotIn('pi_10',blob)

        special=os.path.join(self.tmp.name,'refund?&%#snapshot.db')
        with self.api.get_db() as source, sqlite3.connect(special) as target:source.backup(target)
        before_names=sorted(os.listdir(self.tmp.name));before_stat=os.stat(special)
        with open(special,'rb') as fh:before_hash=hashlib.sha256(fh.read()).hexdigest()
        readonly=tool.open_readonly_snapshot(special)
        try:
            self.assertEqual(readonly.execute('SELECT COUNT(*) FROM refund_attempts').fetchone()[0],1)
            self.assertEqual(os.path.realpath(readonly.execute('PRAGMA database_list').fetchone()[2]),os.path.realpath(special))
        finally:readonly.close()
        after_stat=os.stat(special)
        with open(special,'rb') as fh:after_hash=hashlib.sha256(fh.read()).hexdigest()
        self.assertEqual(sorted(os.listdir(self.tmp.name)),before_names)
        self.assertEqual((after_stat.st_size,after_stat.st_mtime_ns,after_hash),(before_stat.st_size,before_stat.st_mtime_ns,before_hash))
        with open(special+'-wal','wb') as fh:fh.write(b'uncheckpointed')
        with self.assertRaisesRegex(ValueError,'uncheckpointed WAL'):tool.open_readonly_snapshot(special)
    def test_reconciliation_tool_rejects_malformed_metadata_without_exception(self):
        self._crash_child(after_call_start=False)
        attempt=self._attempt()
        malformed=self._exact_evidence(attempt,id='re_malformed_metadata')
        malformed['metadata']=['attempt_id',str(attempt['id'])]
        class RefundApi:
            @staticmethod
            def list(**kwargs):return {'data':[malformed],'has_more':False}
            @staticmethod
            def retrieve(refund_id):raise AssertionError('retrieve must not run')
        tool_path=os.path.join(os.path.dirname(__file__),'tools','reconcile_refund_attempts.py')
        spec=importlib.util.spec_from_file_location('refund_reconcile_tool_malformed',tool_path)
        if spec is None or spec.loader is None:self.fail('could not load reconciliation tool')
        tool=importlib.util.module_from_spec(spec);spec.loader.exec_module(tool)
        with self.api.get_db() as db:
            before=tuple(db.execute('SELECT status,lifecycle_status,manual_review_required FROM refund_attempts WHERE id=?',[attempt['id']]).fetchone())
            report=tool.build_report(db,RefundApi,attempt_id=attempt['id'])
            after=tuple(db.execute('SELECT status,lifecycle_status,manual_review_required FROM refund_attempts WHERE id=?',[attempt['id']]).fetchone())
        self.assertEqual(before,after)
        self.assertEqual(report['attempts'][0]['exact_candidate_count'],0)
        self.assertFalse(report['attempts'][0]['exact_candidate'])


if __name__ == '__main__': unittest.main()
