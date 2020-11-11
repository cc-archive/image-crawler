import json
import logging as log
import multiprocessing
import time
from json import JSONDecodeError
"""
Data structures for throttling our requests to AWS and preventing duplicate work
"""


class LocalTokenBucket:
    """
    A thread-safe token bucket for ensuring conformance to a rate limit.
    (Multiproc-safe)
    """
    def __init__(self, max_val, refresh_rate_sec=1):
        self._refresh_rate_sec = refresh_rate_sec
        self._lock = multiprocessing.Manager().Lock()
        self._last_timestamp = None
        self._MAX_TOKENS = max_val
        self._curr_tokens = multiprocessing.Manager().Value('i', max_val)

    def _acquire_token(self):
        with self._lock:
            now = time.time()
            if not self._last_timestamp:
                self._last_timestamp = now
            elif now - self._last_timestamp > self._refresh_rate_sec:
                # Replenish
                self._curr_tokens = multiprocessing \
                    .Manager() \
                    .Value('i', self._MAX_TOKENS)
                self._last_timestamp = now
            if self._curr_tokens.value <= 0:
                return False
            self._curr_tokens.value -= 1
            return True

    def throttle_fn(self, task_to_throttle):
        """
        Wrap the input function and make it block until it acquires a token from
        the bucket instance. (Thread-safe)
        """
        token_acquired = False
        while not token_acquired:
            token_acquired = self._acquire_token()
            time.sleep(0.1)
        return task_to_throttle()


class RecentlyProcessed:
    """ Determine whether an item has been seen recently. (Multiproc-safe) """
    def __init__(self, retention_num):
        self._queue = multiprocessing.Manager().list()
        self._max_retain = retention_num
        self._lock = multiprocessing.Manager().Lock()

    def seen_recently(self, item):
        with self._lock:
            if item in self._queue:
                return True
            self._queue.append(item)
            if len(self._queue) > self._max_retain:
                self._queue.pop(0)
            return False


def parse_msg(msg):
    try:
        parsed = json.loads(str(msg.value(), 'utf-8'))
        return parsed['identifier']
    except (KeyError, JSONDecodeError):
        log.warning(f'Failed to parse message {msg}')
        return None
