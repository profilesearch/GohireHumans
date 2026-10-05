"""Explainable top-three applicant suggestions using only local, read-only data.

Scores are internal ordering details, never returned. Readiness is the synced
Stripe hint also used by the applications API; hiring still re-checks live.
"""
import re
from urllib.parse import urlsplit


# Common English function words plus every guided-job-draft template word.
STOPWORDS = frozenset('''
    about after again also another because been being between both cannot could
    does doing during either even here into just more most much must only other
    over same should some such than their them then there these they those through
    very want were when where which while whom without would
    needs done type human agent needed suggested deliverable result budget preference
    review before publishing please draft edit scope nothing submitted until post
    listing fixed what with that this from have will your each around words
'''.split())
WORDS = re.compile(r'[a-z0-9]{4,}')


def _keywords(text):
    return set(WORDS.findall((text or '').lower())) - STOPWORDS


def _portfolio_link(value):
    if not isinstance(value, str):
        return False
    try:
        url = urlsplit(value.strip())
        return url.scheme.lower() in ('http', 'https') and bool(url.hostname)
    except ValueError:
        return False


def _score(row, job_keywords):
    message = (row['cover_message'] or '').strip()
    signals = []
    if len(job_keywords & _keywords(message)) >= 2:
        signals.append((3, 'Message addresses your task'))
    if len(message) >= 300:
        signals.append((2, 'Detailed message'))
    if _portfolio_link(row['portfolio_url']) or _portfolio_link(row['profile_portfolio_url']):
        signals.append((2, 'Shared a portfolio link'))
    orders = int(row['total_orders_completed'] or 0)
    if orders > 0:
        signals.append((3, f'Completed {orders} order{"" if orders == 1 else "s"} on GoHireHumans'))
    reviews = int(row['total_reviews'] or 0)
    rating = float(row['avg_rating'] or 0)
    if rating >= 4 and reviews > 0:
        signals.append((2, f'Rated {rating:.1f}/5 ({reviews} review{"" if reviews == 1 else "s"})'))
    if row['is_verified']:
        signals.append((1, 'Verified profile'))
    score = sum(weight for weight, _ in signals) + bool((row['bio'] or '').strip())
    # Stable sorting resolves equal weights in the declared signal order.
    reasons = [reason for _, reason in sorted(signals, key=lambda s: -s[0])][:3]
    return score, reasons or ['Ready to hire']


def suggest(db, job, hiring_enabled, limit=3):
    """Return {application_id: {rank, reasons}}; no writes, transactions or I/O.

    Only active, unbanned, unsuspended, payout-ready pending/shortlisted workers
    on an open/reviewing fixed-price job can appear. Never pad the shortlist.
    """
    job = dict(job)
    if (not hiring_enabled or job.get('budget_type') != 'fixed'
            or job.get('status') not in ('open', 'reviewing') or limit <= 0):
        return {}
    job_keywords = _keywords((job.get('title') or '') + ' ' + (job.get('description') or ''))
    rows = db.execute('''
        SELECT a.id, a.cover_message, a.portfolio_url, a.created_at,
               wp.portfolio_url AS profile_portfolio_url, wp.bio,
               wp.total_orders_completed, wp.avg_rating, wp.total_reviews, wp.is_verified
        FROM applications a
        JOIN users u ON u.id=a.worker_id
        JOIN worker_profiles wp ON wp.user_id=a.worker_id
        WHERE a.job_id=? AND a.status IN ('pending','shortlisted')
          AND u.is_active=1 AND u.is_banned=0 AND u.is_suspended=0
          AND COALESCE(wp.payout_method,'')='stripe_connect_active'
    ''', [job['id']]).fetchall()
    scored = []
    for row in rows:
        score, reasons = _score(row, job_keywords)
        scored.append((score, row['created_at'] or '', row['id'], reasons))
    scored.sort(key=lambda item: (-item[0], item[1], item[2]))
    return {aid: {'rank': rank, 'reasons': reasons}
            for rank, (_, _, aid, reasons) in enumerate(scored[:min(limit, 3)], 1)}
