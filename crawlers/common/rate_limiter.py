import time
import logging

logger = logging.getLogger(__name__)

class RequestBudgetExceeded(RuntimeError):
    """Raised before a request when the per-run budget is exhausted."""

class RateLimiter:
    """
    Rate limiter đơn giản kèm cơ chế exponential backoff khi gặp HTTP 429 hoặc lỗi tạm thời.
    """
    def __init__(self, default_interval: float = 1.5, max_requests: int = None):
        if default_interval < 0 or (max_requests is not None and max_requests < 1):
            raise ValueError("Invalid request interval or budget")
        self.default_interval = default_interval
        self.max_requests = max_requests
        self.request_count = 0
        self.last_call = 0.0
        self.backoff_factor = 1.0

    def wait(self):
        if self.max_requests is not None and self.request_count >= self.max_requests:
            raise RequestBudgetExceeded("Crawler request budget reached")
        elapsed = time.monotonic() - self.last_call
        sleep_time = (self.default_interval * self.backoff_factor) - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)
        self.last_call = time.monotonic()
        self.request_count += 1

    def record_success(self):
        # Reset backoff khi request thành công
        self.backoff_factor = max(1.0, self.backoff_factor * 0.8)

    def record_rate_limit(self, cooldown_seconds: float = 60.0):
        # Khi gặp HTTP 429
        logger.warning(f"Rate limit hit! Sleeping for {cooldown_seconds}s and increasing backoff.")
        self.backoff_factor = min(self.backoff_factor * 2.0, 10.0)
        time.sleep(cooldown_seconds)
