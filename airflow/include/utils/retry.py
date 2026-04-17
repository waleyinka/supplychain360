# utils/retry.py

"""Retry logic for resilient data extraction."""

import time
from functools import wraps
from include.utils.logger import get_logger

logger = get_logger(__name__)


def retry(exceptions, retries: int = 3, delay: int = 2, backoff: int = 2):
    """
    Retry a function with exponential backoff.

    Args:
        exceptions: Tuple of exceptions to catch.
        retries: Number of retries
        delay: Initial delay in seconds
        backoff: Multiplier for delay on each failure.
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            m_retries, m_delay = retries, delay
            while m_retries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    logger.warning(f"{e}, Retrying in {m_delay} seconds...")
                    time.sleep(m_delay)
                    m_retries -= 1
                    m_delay *= backoff
            return f(*args, **kwargs)
        return wrapper
    return decorator