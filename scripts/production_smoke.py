#!/usr/bin/env python3
"""Smoke the committed production image, with no runtime network or credentials."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]


def smoke(sha):
    if not re.fullmatch(r'[a-f0-9]{40}', sha):
        raise ValueError('expected full lowercase commit SHA')
    suffix = uuid.uuid4().hex
    image = 'ghh-release-smoke:' + sha + '-' + suffix
    name = 'ghh-release-smoke-' + suffix
    launched = False
    try:
        # Exclude local ignored secrets/DBs: Docker COPY sees only this commit's
        # backend subtree, never the working directory. No build secrets/args.
        with tempfile.TemporaryFile() as context:
            subprocess.run(['git', 'archive', '--format=tar', sha + ':backend'],
                           cwd=ROOT, stdout=context, check=True, timeout=30)
            context.seek(0)
            subprocess.run(['docker', 'build', '--tag', image, '--file', 'Dockerfile', '-'],
                           stdin=context, check=True, timeout=600)
        launched = True  # docker run may create a container before returning failure
        subprocess.run(['docker', 'run', '--detach', '--name', name, '--network=none',
                        '--env', 'PORT=8080', '--env', 'DATABASE_PATH=/data/gohirehumans.db',
                        '--env', 'ENVIRONMENT=production', '--env', 'ENABLE_AUTO_SEED=0',
                        '--env', 'EMAIL_OUTBOX_WORKER_ENABLED=0',
                        '--env', 'NOTIFICATION_MAINTENANCE_WORKER_ENABLED=0',
                        '--env', 'RAILWAY_GIT_COMMIT_SHA=' + sha, image], check=True, timeout=30)
        deadline = time.monotonic() + 100
        while time.monotonic() < deadline:
            result = subprocess.run(['docker', 'inspect', '--format', '{{json .State}}', name],
                                    check=True, capture_output=True, text=True, timeout=15)
            state = json.loads(result.stdout)
            health = state.get('Health', {}).get('Status')
            if state['Status'] != 'running' or health not in {'starting', 'healthy'}:
                raise RuntimeError('production container exited, unhealthy, or missing HEALTHCHECK')
            if health == 'healthy':
                # No CMD, USER, or HEALTHCHECK override. Probe from the isolated
                # container's loopback; /data is its disposable writable layer.
                probe = ("import json,os,sqlite3,urllib.request; "
                         "r=urllib.request.urlopen('http://127.0.0.1:8080/health',timeout=5); "
                         "d=json.load(r); "
                         "assert r.status==200 and d['status']=='ok'; "
                         "assert d['service']=='gohirehumans-api'; "
                         "assert d['version']==os.environ['RAILWAY_GIT_COMMIT_SHA'][:7]; "
                         "assert os.getuid()!=0; "
                         "assert os.path.isfile('/data/gohirehumans.db'); "
                         "db=sqlite3.connect('file:/data/gohirehumans.db?mode=ro',uri=True); "
                         "assert db.execute('PRAGMA quick_check').fetchone()==('ok',); "
                         "assert db.execute('SELECT count(*) FROM notification_worker_leases').fetchone()==(0,); "
                         "db.close()")
                subprocess.run(['docker', 'exec', name, 'python', '-c', probe],
                               check=True, timeout=15)
                print('Production image startup, HEALTHCHECK, identity and non-root DB: OK')
                return
            time.sleep(2)
        raise RuntimeError('production container health timed out')
    finally:
        # Preserve the original error but never report success if cleanup fails.
        failed = sys.exc_info()[0] is not None
        cleanup_errors = []
        cleanup = ([['docker', 'rm', '-f', name]] if launched else [])
        cleanup.append(['docker', 'image', 'rm', '-f', image])
        for command in cleanup:
            try:
                result = subprocess.run(command, check=False, timeout=30)
                if result.returncode:
                    cleanup_errors.append(command[1])
            except (OSError, subprocess.TimeoutExpired):
                cleanup_errors.append(command[1])
        if cleanup_errors:
            print('Smoke cleanup failed: ' + ', '.join(cleanup_errors), file=sys.stderr)
            if not failed:
                raise RuntimeError('smoke resource cleanup failed')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sha', required=True)
    smoke(parser.parse_args().sha)
