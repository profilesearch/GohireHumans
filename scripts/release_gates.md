# Release gates

`release_preflight.py` is a **read-only evidence collector**, not deployment
authorization, branch protection, or a sender shutdown action. It only performs
GitHub GETs. It never runs the notification/shutdown monitor: that monitor can
mutate production and is intentionally paused/disabled.

## Required sequence

1. Freeze and review the complete candidate. Run all local backend, script,
   static, security, and browser gates. Do not equate mocked Docker tests with
   execution of the production container.
2. Require `backend-tests`, `static-frontend-checks`, and
   `browser-regression-checks` to finish successfully on the exact candidate SHA.
   In PR CI, `github.sha` is the tested synthetic merge commit; record that SHA
   separately from the PR head. Revalidate against current base/head before merge.
3. Run the read-only collector against the exact full lowercase commit SHA:

   ```sh
   python scripts/release_preflight.py --repo profilesearch/GohireHumans --sha FULL_SHA
   ```

   Missing, pending, skipped, cancelled, failed, malformed, or unavailable check
   evidence blocks release. Use `--require-status EXACT_CONTEXT` (repeatable) only
   for deployment contexts verified by readback on that SHA. Never infer Railway
   readiness from GitHub CI or from a Vercel status. Authentication requires only
   read access; tokens are not printed.
4. Independently read back the paused/disabled local sender job, cron/timer, and
   supervisor state. Keep it stopped. Read-only configuration/status inspection
   is allowed; **do not execute the mutation-capable shutdown monitor**, including
   purported check/preflight modes, and do not restart any sender.
5. Hold merge until actual Ubuntu Docker CI passes. After merging, require the
   three checks on the merge SHA, successful intended deployment statuses,
   backend `/health` version matching that SHA, and public frontend verification.
   A local pass does not enforce server-side policy or attest a live deployment.

## Production container smoke boundary

```sh
python scripts/production_smoke.py --sha FULL_COMMITTED_SHA
```

Requires Git, Python 3.12, and a local Docker daemon (GitHub Actions Ubuntu).
Build input is `git archive FULL_COMMITTED_SHA:backend`, using its unchanged
`Dockerfile`, dependencies, `CMD`, runtime `USER`, and `HEALTHCHECK`. Untracked
workspace secrets and databases cannot enter the context. Do not commit secrets
or database snapshots; tracked contents are part of the production image.

Image build downloads the base image/dependencies; **runtime** has
`--network=none`, no published ports, mounts, env files, forwarded credentials,
or command/healthcheck overrides. The seven explicit environment settings select
port 8080, production mode, `/data/gohirehumans.db`, the revision, disabled seeding,
and both disabled notification workers. `/data` is the disposable container layer,
not a host or production volume. This path matches backend precedence: an explicit
`DATABASE_PATH=/data/smoke.db` would NOT win over writable `/data/gohirehumans.db`.

Wait for Docker's real HEALTHCHECK, then probe health identity, non-root execution,
SQLite integrity and absence of worker leases over container-local access. Use
unique image/container names and bounded subprocess/health timeouts; clean both
on success and failure, with cleanup failure blocking success. Build caches remain
under the runner's Docker lifecycle. This is fresh-database startup coverage, not
production-data migration, provider delivery, or attached-volume coverage.

## Administrative changes

Branch protection and Railway Wait for CI are separate approval-gated administrative
changes. This implementation changes neither. The audit's local `release.md`
contains the exact proposed settings and readback limitations; obtain explicit
approval before changing them and verify the target after any authorized write.
