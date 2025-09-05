import logging
import random
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("netopt")


def create_optimized_session(pool_maxsize: int = 100) -> requests.Session:
    """Return a requests.Session with a large connection pool and keep-alive."""
    sess = requests.Session()
    # Disable built-in retries; we'll handle retries manually.
    retry = Retry(total=0)
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=pool_maxsize,
        pool_maxsize=pool_maxsize,
    )
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/116.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "id,en;q=0.9",
        "Connection": "keep-alive",
    })
    return sess


def timed_request(sess: requests.Session, method: str, url: str, **kwargs):
    """Perform request and log connect+TTFB and total latency."""
    start = time.perf_counter()
    resp = sess.request(method, url, **kwargs)
    ttfb = resp.elapsed.total_seconds()
    total = time.perf_counter() - start
    log.info("latency %s %s connect+ttfb=%.3f total=%.3f", method, url, ttfb, total)
    return resp, ttfb, total


def prewarm_session(sess: requests.Session, base_url: str):
    """Warm up TLS handshake and cookies."""
    urls = [
        base_url,
        f"{base_url}/member/booking",
    ]
    for u in urls:
        try:
            timed_request(sess, "GET", u, timeout=5)
        except Exception:
            pass


def decorrelated_jitter(previous: float, cap: float = 1.0, base: float = 0.1) -> float:
    """Decorrelated jitter backoff."""
    if previous <= 0:
        previous = base
    return min(cap, random.uniform(base, previous * 3))


def short_window_aggressive(attempt_fn, attempts: int = 3, base_delay: float = 0.1):
    """Retry helper for the release window using decorrelated jitter."""
    delay = base_delay
    last = (False, "no-attempt", 0.0, None)
    for _ in range(attempts):
        last = attempt_fn()
        if last[0]:
            return last
        delay = decorrelated_jitter(delay)
        time.sleep(delay)
    return last
