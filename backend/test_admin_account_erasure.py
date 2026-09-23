"""End-to-end regressions for the step-up account erasure endpoint."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock


class AdminAccountErasureTests(unittest.TestCase):
    EMAIL = 'erase-me@example.test'
    NAME = 'Unique Erasure Person'
    PASSWORD = 'Old-Worker-Password-123!'
    ADMIN_PASSWORD = 'Admin-Password-123!'

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env = mock.patch.dict(os.environ, {
            'DATABASE_PATH': str(Path(self.temp.name) / 'erasure.db'),
            'DISABLE_AUTO_SEED': '1', 'AGENT_PLATFORM_DOMAINS': '',
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        spec = importlib.util.spec_from_file_location('erasure_core', Path(__file__).with_name('api_core.py'))
        assert spec is not None and spec.loader is not None
        self.core = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.core)
        self.core.init_db()
        with self.core.get_db() as db:
            db.executemany('INSERT INTO users (id,email,name,password_hash,is_admin,is_active) VALUES (?,?,?,?,?,?)', [
                (1, 'admin@example.test', 'Admin', self.core.hash_password(self.ADMIN_PASSWORD), 1, 1),
                (2, self.EMAIL, self.NAME, self.core.hash_password(self.PASSWORD), 0, 0),
                (3, 'peer@example.test', 'Peer', self.core.hash_password(self.PASSWORD), 0, 1),
            ])
            db.executemany("INSERT INTO sessions(user_id,token,expires_at) VALUES (?,?,datetime('now','+1 day'))", [(1,'admin-session'),(2,'old-session'),(3,'peer-session')])
            db.commit()

    def request(self, method, path, body=None, token='admin-session'):
        ctx = self.core._request_ctx
        for key in ('body_cache', 'raw_body'):
            if hasattr(ctx, key):
                delattr(ctx, key)
        data = json.dumps(body or {})
        ctx.request_method, ctx.path_info, ctx.query_string = method, path, ''
        ctx.http_authorization = 'Bearer ' + token if token else ''
        ctx.http_x_api_key = ''
        ctx.stdin_data, ctx.stdin_data_raw = data, data.encode()
        ctx.content_type, ctx.content_length, ctx.remote_addr = 'application/json', str(len(data.encode())), '127.0.0.1'
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.core.handle_request()
        headers, _, text = output.getvalue().partition('\n\n')
        status = next((int(line.split(':',1)[1]) for line in headers.splitlines() if line.startswith('Status:')), 200)
        return status, json.loads(text or '{}')

    def erase(self, **fields):
        return self.request('POST', '/admin/users/2/erase', {'admin_password': self.ADMIN_PASSWORD, **fields})

    def dump(self):
        with self.core.get_db() as db:
            return '\n'.join(db.iterdump())

    def seed_personal(self):
        with self.core.get_db() as db:
            db.execute("INSERT INTO worker_profiles(user_id,bio,skills,payout_account_id,payout_method_details,portfolio_url) VALUES (2,?,?,?,? ,?)", (self.NAME, json.dumps([self.NAME]), 'acct_sim_2', self.EMAIL, self.EMAIL))
            db.execute("INSERT INTO employer_profiles(user_id,company_name,description,website) VALUES (2,?,?,?)", (self.NAME, self.EMAIL, self.EMAIL))
            db.execute("INSERT INTO services(worker_id,title,description,category) VALUES (2,?,?, 'research')", (self.NAME,self.EMAIL))
            service_id = db.execute('SELECT last_insert_rowid()').fetchone()[0]
            db.execute("INSERT INTO jobs(employer_id,title,description,category,budget_amount) VALUES (2,?,?, 'research',25)", (self.NAME,self.EMAIL))
            job_id = db.execute('SELECT last_insert_rowid()').fetchone()[0]
            db.execute("INSERT INTO applications(job_id,worker_id,cover_message) VALUES (?,3,?)", (job_id,self.EMAIL))
            db.execute("INSERT INTO job_application_views(job_id,employer_id,first_viewed_at,last_viewed_at) VALUES (?,2,datetime('now'),datetime('now'))", (job_id,))
            db.execute("INSERT INTO job_application_reminders(job_id,employer_id,reminder_kind,cohort_started_at,cohort_first_application_id,cohort_last_application_id,application_count) VALUES (?,2,'24h',datetime('now'),1,1,1)", (job_id,))
            db.execute("INSERT INTO jobs(employer_id,title,description,category,budget_amount) VALUES (3,'Other job','Other','research',25)")
            peer_job = db.execute('SELECT last_insert_rowid()').fetchone()[0]
            db.execute('INSERT INTO applications(job_id,worker_id,cover_message) VALUES (?,2,?)', (peer_job,self.NAME))
            db.execute("INSERT INTO api_keys(user_id,key_hash,key_prefix,name) VALUES (2,'hash-erasure','ghh_test',?)", (self.NAME,))
            key_id = db.execute('SELECT last_insert_rowid()').fetchone()[0]
            db.execute("INSERT INTO api_key_usage(api_key_id,endpoint,method,error_message) VALUES (?,'/me','GET',?)", (key_id,self.EMAIL))
            db.execute("INSERT INTO password_reset_tokens(user_id,token_hash,expires_at) VALUES (2,?,datetime('now','+1 day'))", ('a'*64,))
            db.execute('INSERT INTO notifications(user_id,type,title,message) VALUES (2,\'test\',?,?)', (self.NAME,self.EMAIL))
            db.execute("INSERT INTO transactional_email_outbox(user_id,email_to,notification_type,title,message,dedupe_context,dedupe_key,expires_at,provider_email_id) VALUES (2,?,'test',?,?,'x','unique-erasure-key',datetime('now','+1 day'),'provider-erasure')", (self.EMAIL,self.NAME,self.EMAIL))
            db.execute("INSERT INTO transactional_email_delivery_events(provider_event_id,provider_email_id,event_type,payload_sha256) VALUES ('event-erasure','provider-erasure','sent',?)", ('a'*64,))
            outbox_id = db.execute("SELECT id FROM transactional_email_outbox WHERE dedupe_key='unique-erasure-key'").fetchone()[0]
            db.execute("INSERT INTO agentmail_send_ledger(key_digest,outbox_id,fingerprint,state) VALUES (?,?,?,'approved')", ('b'*64,outbox_id,'c'*64))
            db.execute('INSERT INTO referrals(referrer_id,referred_id) VALUES (3,2)')
            db.execute('INSERT INTO referrals(referrer_id,referred_id) VALUES (2,3)')
            db.execute('UPDATE users SET referred_by=2 WHERE id=3')
            db.execute("INSERT INTO audit_log(user_id,action,entity_type,entity_id,details) VALUES (3,'prior','user',2,?)", (json.dumps({'nested': {'email': self.EMAIL.upper(), 'name': self.NAME}, 'message': 'Contact '+self.EMAIL}),))
            db.execute("INSERT INTO audit_log(user_id,action,entity_type,entity_id,details) VALUES (3,'other','user',3,?)", (json.dumps({'message': self.EMAIL+' / '+self.NAME}),))
            db.commit()
            return service_id

    def test_default_dry_run_reports_counts_without_any_database_write(self):
        self.seed_personal()
        before = self.dump()
        status, body = self.erase()
        self.assertEqual(status, 200, body)
        self.assertTrue(body['dry_run'])
        self.assertTrue(body['eligible'])
        self.assertEqual(body['blockers'], [])
        self.assertEqual(body['will_delete']['services'], 1)
        self.assertEqual(body['will_delete']['applications'], 2)
        self.assertEqual(body['retained']['agentmail_send_ledger'], 1)
        self.assertEqual(body['will_anonymize']['users'], 1)
        self.assertEqual(self.dump(), before)

    def test_non_admin_denied(self):
        self.assertEqual(self.request('POST','/admin/users/2/erase', {}, 'peer-session')[0], 403)

    def test_missing_admin_step_up_denied(self):
        status, body = self.request('POST','/admin/users/2/erase')
        self.assertEqual(status,403,body)
        self.assertIn('Admin password',body['error'])

    def test_active_user_blocked_without_writes(self):
        with self.core.get_db() as db:
            db.execute('UPDATE users SET is_active=1 WHERE id=2')
            db.commit()
        before = self.dump()
        self.assertEqual(self.erase(dry_run=False,confirm_email=self.EMAIL)[0],409)
        self.assertEqual(self.dump(),before)

    def test_admin_target_blocked_without_writes(self):
        before = self.dump()
        status, body = self.request('POST','/admin/users/1/erase',{'admin_password':self.ADMIN_PASSWORD,'dry_run':False,'confirm_email':'admin@example.test'})
        self.assertEqual(status,409,body)
        self.assertEqual(self.dump(),before)

    def test_orders_block_both_participant_roles(self):
        with self.core.get_db() as db:
            db.execute("INSERT INTO orders(type,worker_id,employer_id,total_amount) VALUES ('job_hire',2,3,20)")
            db.commit()
        before = self.dump()
        status, body = self.erase(dry_run=False,confirm_email=self.EMAIL)
        self.assertEqual(status,409,body)
        self.assertEqual(self.dump(),before)
        with self.core.get_db() as db:
            db.execute('DELETE FROM orders')
            db.execute("INSERT INTO orders(type,worker_id,employer_id,total_amount) VALUES ('job_hire',3,2,20)")
            db.commit()
        before = self.dump()
        self.assertEqual(self.erase(dry_run=False,confirm_email=self.EMAIL)[0],409)
        self.assertEqual(self.dump(),before)

    def test_live_stripe_payout_account_blocked(self):
        with self.core.get_db() as db:
            db.execute("INSERT INTO worker_profiles(user_id,payout_account_id) VALUES (2,'acct_live_erasure')")
            db.commit()
        before = self.dump()
        status, body = self.erase(dry_run=False,confirm_email=self.EMAIL)
        self.assertEqual(status,409,body)
        self.assertIn('stripe_account_present',json.dumps(body))
        self.assertEqual(self.dump(),before)

    def test_wrong_email_denied_without_writes(self):
        before = self.dump()
        self.assertIn(self.erase(dry_run=False,confirm_email='other@example.test')[0],(400,409))
        self.assertEqual(self.dump(),before)

    def test_successful_erase_revokes_and_scrubs_every_text_column(self):
        service_id = self.seed_personal()
        status, body = self.erase(dry_run=False,confirm_email=self.EMAIL.upper())
        self.assertEqual(status,200,body)
        self.assertTrue(body['erased'])
        self.assertEqual(body['deleted']['services'],1)
        with self.core.get_db() as db:
            row = db.execute('SELECT * FROM users WHERE id=2').fetchone()
            self.assertEqual((row['email'],row['name'],row['password_hash'],row['is_active']),('erased-user-2@erased.invalid','Deleted user','',0))
            self.assertIsNone(row['google_sub'])
            self.assertIsNone(row['avatar_url'])
            self.assertIsNone(row['referral_code'])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM audit_log WHERE action='admin_erase_user' AND entity_id=2").fetchone()[0],1)
            self.assertIsNone(db.execute('SELECT referred_by FROM users WHERE id=3').fetchone()[0])
            self.assertEqual(db.execute('SELECT COUNT(*) FROM referrals').fetchone()[0], 0)
            for (table,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall():
                columns = [c['name'] for c in db.execute(f'PRAGMA table_info("{table}")') if 'TEXT' in c['type'].upper()]
                for column in columns:
                    values = [r[0] for r in db.execute(f'SELECT "{column}" FROM "{table}" WHERE "{column}" IS NOT NULL')]
                    for value in values:
                        self.assertNotIn(self.EMAIL.lower(),value.lower(),(table,column))
                        self.assertNotIn(self.NAME.lower(),value.lower(),(table,column))
            db.execute('PRAGMA foreign_key_check')
            self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(),[])
        self.assertEqual(self.request('GET','/notifications',token='old-session')[0],401)
        self.assertFalse(self.core.verify_password(self.PASSWORD, ''))
        self.assertEqual(self.request('POST','/auth/login',{'email':self.EMAIL,'password':self.PASSWORD},token='')[0],401)
        self.assertEqual(self.request('POST','/auth/login',{'email':'erased-user-2@erased.invalid','password':self.PASSWORD},token='')[0],401)
        status, registered = self.request('POST','/auth/register',{'email':self.EMAIL,'name':'Replacement','password':'New-Password-123!'},token='')
        self.assertEqual(status,201,registered)
        self.assertNotEqual(registered['id'],2)
        self.assertEqual(self.request('GET',f'/services/{service_id}',token='')[0],404)
        status, body = self.erase(dry_run=False,confirm_email='erased-user-2@erased.invalid')
        self.assertEqual(status,409,body)
        self.assertIn('already_erased',json.dumps(body))

    def test_escaped_audit_json_identity_is_redacted(self):
        with self.core.get_db() as db:
            # Legacy details can contain JSON-escaped characters, even in an
            # unrelated actor's row with no literal email bytes in the DB.
            escaped = json.dumps({'email': self.EMAIL.replace('@', '\\u0040'), 'name': self.NAME})
            escaped = escaped.replace('\\\\u0040', '\\u0040')
            db.execute("INSERT INTO audit_log(user_id,action,entity_type,entity_id,details) VALUES (3,'legacy','other',3,?)", (escaped,))
            db.commit()
        status, body = self.erase(dry_run=False,confirm_email=self.EMAIL)
        self.assertEqual(status, 200, body)
        with self.core.get_db() as db:
            details = db.execute("SELECT details FROM audit_log WHERE action='legacy'").fetchone()[0]
            self.assertNotIn(self.EMAIL, json.dumps(json.loads(details), ensure_ascii=False))
            self.assertNotIn(self.NAME, json.dumps(json.loads(details), ensure_ascii=False))

    def test_quoted_name_in_audit_json_is_redacted_without_corrupting_json(self):
        quoted_name = self.NAME + ' "Quoted"'
        with self.core.get_db() as db:
            db.execute('UPDATE users SET name=? WHERE id=2', (quoted_name,))
            db.execute("INSERT INTO audit_log(user_id,action,entity_type,entity_id,details) VALUES (2,'quoted','user',2,?)", (json.dumps({'name': quoted_name}),))
            db.commit()
        status, body = self.erase(dry_run=False,confirm_email=self.EMAIL)
        self.assertEqual(status, 200, body)
        with self.core.get_db() as db:
            details = db.execute("SELECT details FROM audit_log WHERE action='quoted'").fetchone()[0]
            self.assertNotIn(quoted_name, json.loads(details)['name'])

    def test_financial_ledgers_block_without_order(self):
        with self.core.get_db() as db:
            db.execute("INSERT INTO payment_setup_operations(operation_key,operation_kind,user_id,request_fingerprint,request_binding_json,processor_idempotency_key,status) VALUES ('erase-op','employer',2,'fp','{}','erase-key','prepared')")
            db.commit()
        before = self.dump()
        status, body = self.erase(dry_run=False,confirm_email=self.EMAIL)
        self.assertEqual(status,409,body)
        self.assertEqual(self.dump(),before)


class AdminAccountErasureReviewRegressions(AdminAccountErasureTests):
    """Blockers from independent review of 9f3697d."""

    def test_peer_notification_and_email_naming_target_are_scrubbed(self):
        with self.core.get_db() as db:
            db.execute('UPDATE users SET is_active=1 WHERE id=2')
            job_id = db.execute("INSERT INTO jobs(employer_id,title,description,category,budget_amount) VALUES (3,'Peer job','Job','research',25)").lastrowid
            db.execute("INSERT INTO notifications(user_id,type,title,message) VALUES (3,'other','Annual plan','Unique Erasurement Personality stays')")
            db.commit()
        status, _ = self.request('POST', f'/jobs/{job_id}/apply', {'cover_message': 'work'}, 'old-session')
        self.assertEqual(status, 201)
        with self.core.get_db() as db:
            named = db.execute("SELECT COUNT(*) FROM notifications WHERE user_id=3 AND message LIKE ?", ['%' + self.NAME + '%']).fetchone()[0]
            self.assertGreaterEqual(named, 1)
            db.execute('UPDATE users SET is_active=0 WHERE id=2')
            db.commit()
        status, body = self.erase(dry_run=False, confirm_email=self.EMAIL)
        self.assertEqual(status, 200, body)
        with self.core.get_db() as db:
            for table in ('notifications', 'transactional_email_outbox'):
                for row in db.execute(f'SELECT title,message FROM {table} WHERE user_id=3'):
                    text = (row['title'] or '') + ' ' + (row['message'] or '')
                    self.assertNotIn(self.NAME.lower(), text.lower())
                    self.assertNotIn(self.EMAIL.lower(), text.lower())
            self.assertEqual(db.execute("SELECT COUNT(*) FROM notifications WHERE user_id=3 AND message LIKE 'A former user applied%'").fetchone()[0], 1)
            # Word-bounded: unrelated text that merely contains the name as a substring is untouched.
            self.assertEqual(db.execute("SELECT message FROM notifications WHERE title='Annual plan'").fetchone()[0], 'Unique Erasurement Personality stays')

    def test_login_audit_ip_is_removed(self):
        ctx = self.core._request_ctx
        with mock.patch.object(self.core, 'check_rate_limit', side_effect=lambda: setattr(ctx, 'remote_addr', '203.0.113.72') or True):
            status, _ = self.request('POST', '/auth/login', {'email': self.EMAIL, 'password': 'wrong'}, token='')
        self.assertEqual(status, 401)
        status, body = self.erase(dry_run=False, confirm_email=self.EMAIL)
        self.assertEqual(status, 200, body)
        with self.core.get_db() as db:
            for (details,) in db.execute("SELECT details FROM audit_log WHERE details IS NOT NULL"):
                self.assertNotIn('203.0.113.72', details)
                self.assertNotIn(self.EMAIL, details)

    def test_erasing_mail_history_does_not_consume_reset_budget(self):
        with self.core.get_db() as db:
            db.execute('UPDATE users SET is_active=1 WHERE id=3')
            for i in range(11):
                oid = db.execute("INSERT INTO transactional_email_outbox(user_id,email_to,notification_type,title,message,dedupe_context,dedupe_key,expires_at) VALUES (2,?,'new_application',?,?,'x',?,datetime('now','+1 day'))", [self.EMAIL, self.NAME, self.EMAIL, f'old-{i}']).lastrowid
                db.execute("INSERT INTO agentmail_send_ledger(key_digest,outbox_id,fingerprint,state,prepared_at,provider_id,message_id,thread_id) VALUES (?,?,?,'accepted',datetime('now'),'agentmail:opaque',?,?)", [f'{i:064x}', oid, 'f' * 64, f'm{i}', f't{i}'])
            db.commit()
        status, body = self.erase(dry_run=False, confirm_email=self.EMAIL)
        self.assertEqual(status, 200, body)
        with self.core.get_db() as db:
            orphans = db.execute('SELECT COUNT(*) FROM agentmail_send_ledger l LEFT JOIN transactional_email_outbox o ON o.id=l.outbox_id WHERE o.id IS NULL').fetchone()[0]
            self.assertEqual(orphans, 0)
            kept = db.execute("SELECT DISTINCT notification_type, email_to, title, message FROM transactional_email_outbox WHERE user_id=2").fetchall()
            self.assertEqual([tuple(r) for r in kept], [('new_application', 'erased-user-2@erased.invalid', '[erased]', '[erased]')])
        env = {'PASSWORD_RESET_EMAIL_ENABLED': 'true', 'PASSWORD_RESET_ENCRYPTION_KEY': 'a' * 64,
               'AGENTMAIL_API_KEY': 'offline-key', 'AGENTMAIL_INBOX_ID': 'gohirehumans.operations@agentmail.to',
               'PASSWORD_RESET_DAILY_SEND_CAP': '10', 'EMAIL_PROVIDER': 'agentmail', 'AGENTMAIL_SEND_ENABLED': 'false'}
        with mock.patch.dict(os.environ, env), mock.patch.object(self.core, 'PASSWORD_RESET_RESPONSE_FLOOR_SECONDS', 0):
            status, _ = self.request('POST', '/auth/forgot-password', {'email': 'peer@example.test'}, token='')
            self.assertEqual(status, 200)
            with self.core.get_db() as db, mock.patch.object(self.core.agentmail_transport.urllib.request, 'build_opener') as opener:
                self.core.flush_transactional_notification_emails(db, only_types=('password_reset',))
                self.assertEqual(opener.call_count, 1, 'reset mail must reach the provider, not be suppressed by erased history')
