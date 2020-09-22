import json
import logging as log
import threading
import time
from json import JSONDecodeError
"""
Data structures for throttling our requests to AWS and preventing duplicate work
"""


class LocalTokenBucket:
    """
    A thread-safe token bucket for ensuring conformance to a rate limit.
    """
    def __init__(self, max_val, refresh_rate_sec=1):
        self._refresh_rate_sec = refresh_rate_sec
        self._lock = threading.Lock()
        self._last_timestamp = None
        self._MAX_TOKENS = max_val
        self._curr_tokens = max_val

    def _acquire_token(self):
        with self._lock:
            now = time.time()
            if not self._last_timestamp:
                self._last_timestamp = now
            elif now - self._last_timestamp > self._refresh_rate_sec:
                # Replenish
                self._curr_tokens = self._MAX_TOKENS
                self._last_timestamp = now
            if self._curr_tokens <= 0:
                return False
            self._curr_tokens -= 1
            return True

    def throttle_fn(self, task_to_throttle):
        """
        Wrap the input function and make it block until it acquires a token from
        the bucket instance. (Thread-safe)
        """
        token_acquired = False
        while not token_acquired:
            token_acquired = self._acquire_token()
            time.sleep(0.01)
        return task_to_throttle()


class RecentlyProcessed:
    """ Determine whether an item has been seen recently. (Thread-safe) """
    def __init__(self, retention_num):
        # For fast membership checks
        self._set = set()
        # For remembering order of arrival
        self._queue = []
        self._max_retain = retention_num
        self._lock = threading.Lock()

    def seen_recently(self, item):
        with self._lock:
            if item in self._set:
                return True
            self._set.add(item)
            self._queue.append(item)
            if len(self._queue) > self._max_retain:
                to_delete = self._queue.pop(0)
                self._set.remove(to_delete)
            return False


def parse_msg(msg):
    try:
        parsed = json.loads(str(msg.value(), 'utf-8'))
        return parsed['identifier']
    except (KeyError, JSONDecodeError):
        log.warning(f'Failed to parse message {msg}')
        return None
