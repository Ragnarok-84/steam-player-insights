import time
import logging

logger = logging.getLogger(__name__)

class RateLimiter:
    """
    Rate limiter đơn giản kèm cơ chế exponential backoff khi gặp HTTP 429 hoặc lỗi tạm thời.
    """
    def __init__(self, default_interval: float = 1.5):
        self.default_interval = default_interval
        self.last_call = 0.0
        self.backoff_factor = 1.0

    def wait(self):
        elapsed = time.time() - self.last_call
        sleep_time = (self.default_interval * self.backoff_factor) - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)
        self.last_call = time.time()

    def record_success(self):
        # Reset backoff khi request thành công
        self.backoff_factor = max(1.0, self.backoff_factor * 0.8)

    def record_rate_limit(self, cooldown_seconds: float = 60.0):
        # Khi gặp HTTP 429
        logger.warning(f"Rate limit hit! Sleeping for {cooldown_seconds}s and increasing backoff.")
        self.backoff_factor = min(self.backoff_factor * 2.0, 10.0)
        time.sleep(cooldown_seconds)
