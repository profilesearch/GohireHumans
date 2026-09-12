#!/usr/bin/env python3
"""Read-only, fail-closed checks for an explicit immutable release SHA.

This is evidence collection, not branch protection or deployment authorization.
See release_gates.md for mandatory local sender and deployment gates.
"""
import argparse
import json
import os
import re
import urllib.error
import urllib.request

REQUIRED_CHECKS = ('backend-tests', 'static-frontend-checks', 'browser-regression-checks')


def github_get(path):
    headers = {'Accept': 'application/vnd.github+json',
               'X-GitHub-Api-Version': '2022-11-28', 'User-Agent': 'ghh-release-preflight'}
    token = os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN')
    if token:
        headers['Authorization'] = 'Bearer ' + token
    request = urllib.request.Request('https://api.github.com/repos/' + path,
                                     headers=headers, method='GET')
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', required=True, help='owner/repository')
    parser.add_argument('--sha', required=True, help='full lowercase 40-character commit SHA')
    parser.add_argument('--require-status', action='append', default=[],
                        help='also require this deployment status context; repeatable')
    args = parser.parse_args(argv)
    if not re.fullmatch(r'[a-f0-9]{40}', args.sha):
        parser.error('--sha must be a full lowercase 40-character commit SHA, not a ref')
    if not re.fullmatch(r'[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+', args.repo):
        parser.error('--repo must be owner/repository')
    root = f'{args.repo}/commits/{args.sha}'
    try:
        checks = []
        seen_ids = set()
        expected_count = None
        for page in range(1, 101):
            data = github_get(f'{root}/check-runs?filter=latest&per_page=100&page={page}')
            total = data['total_count']
            batch = data['check_runs']
            if type(total) is not int or total < 0 or not isinstance(batch, list):
                raise ValueError('malformed check-run response')
            if expected_count is not None and total != expected_count:
                raise ValueError('check-run count changed; retry preflight')
            expected_count = total
            for check in batch:
                if not isinstance(check, dict) or type(check.get('id')) is not int:
                    raise ValueError('malformed check run')
                if check['id'] in seen_ids:
                    raise ValueError('duplicate check run; retry preflight')
                seen_ids.add(check['id'])
            checks.extend(batch)
            if len(checks) == data['total_count']:
                break
            if not data['check_runs'] or len(checks) > data['total_count']:
                raise ValueError('inconsistent check-run count; retry preflight')
        else:
            raise ValueError('check-run pagination limit reached')
        failures = check_failures(args.sha, checks, REQUIRED_CHECKS)
        if args.require_status:
            statuses = {}
            for page in range(1, 101):
                batch = github_get(f'{root}/statuses?per_page=100&page={page}')
                for status in batch:  # GitHub returns newest first; never accept stale green.
                    statuses.setdefault(status['context'], status['state'])
                if len(batch) < 100:
                    break
            else:
                raise ValueError('status pagination limit reached')
            failures += [f'{name}: missing or unsuccessful deployment status'
                         for name in args.require_status if statuses.get(name) != 'success']
        print(json.dumps({'sha': args.sha, 'checks_read': len(checks),
                          'ok': not failures, 'failures': failures}, indent=2))
        return int(bool(failures))
    except (urllib.error.URLError, OSError, ValueError, KeyError, TypeError) as exc:
        # Do not print headers, tokens, response bodies, or authenticated URLs.
        print(json.dumps({'sha': args.sha, 'ok': False, 'error': type(exc).__name__}))
        return 1


def check_failures(sha, checks, required):
    failures = []
    for name in required:
        matches = [c for c in checks if c.get('name') == name and c.get('head_sha') == sha]
        if not matches or any(c.get('status') != 'completed' or c.get('conclusion') != 'success'
                              for c in matches):
            failures.append(f'{name}: missing, pending, or unsuccessful on {sha}')
    return failures


if __name__ == '__main__':
    raise SystemExit(main())
