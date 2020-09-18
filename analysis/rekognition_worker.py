import boto3
import botocore.exceptions
import logging as log
import time
import threading
import json
import concurrent.futures
import enum
from functools import partial
from json.decoder import JSONDecodeError
from collections import defaultdict, Counter

IMG_BUCKET = 'cc-image-analysis'
LABELS_TOPIC = 'image_analysis_labels'
# Used to respect AWS service limits
MAX_REKOGNITION_RPS = 50
# Number of pending messages to store simultaneously in memory
NUM_MESSAGES_BUFFER = 1500
MAX_PENDING_FUTURES = 1000
# These are network-bound threads, it's OK to use a lot of them
NUM_THREADS = 50
# Number of recently processed image IDs to retain for duplication prevention
NUM_RECENT_IMAGE_ID_RETENTION = 10000
# Deduplication set


class TaskStatus(enum.Enum):
    SUCCEEDED = 1
    IGNORED_DUPLICATE = 2
    ERROR = 3


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


def detect_labels_query(image_uuid, boto3_session):
    img = {
        'S3Object': {
            'Bucket': 'cc-image-analysis',
            'Name': f'{image_uuid}.jpg'
        }
    }
    rekog_client = boto3_session.client('rekognition')
    response = rekog_client.detect_labels(Image=img)
    data = {
        'image_uuid': image_uuid,
        'response': response
    }
    return data


def enqueue(rekognition_response: dict, kafka_producer):
    resp_json = json.dumps(rekognition_response).encode('utf-8')
    kafka_producer.produce(LABELS_TOPIC, resp_json)


def handle_image_task(message, output_producer, recent_ids):
    """
    Get Rekognition labels for an image and output results to a Kafka topic.

    Only call Rekognition if we haven't recently processed the ID.
    """
    msg = json.loads(message)
    image_uuid = msg['identifier']
    if recent_ids.seen_recently(image_uuid):
        return TaskStatus.IGNORED_DUPLICATE
    boto3_session = boto3.session.Session()
    response = detect_labels_query(image_uuid, boto3_session)
    enqueue(response, output_producer)
    return TaskStatus.SUCCEEDED


def _monitor_futures(futures):
    """
    Summarizes task progress and handles exceptions. Returns a list of pending
    futures (with completed futures removed).
    """
    _futures = []
    statuses = defaultdict(int)
    for f in futures:
        if f.done():
            try:
                res = f.result()
                statuses[res] += 1
            except botocore.exceptions.ClientError:
                log.warning("Boto3 failure: ", exc_info=True)
                statuses[TaskStatus.ERROR] += 1
        else:
            # Preserve pending futures
            _futures.append(f)
    return _futures, Counter(statuses)


def _parse_msg(msg):
    try:
        parsed = json.loads(str(msg.value(), 'utf-8'))
        return parsed['identifier']
    except (KeyError, JSONDecodeError):
        log.warning(f'Failed to parse message {msg}')
        return None


def _poll_work(msg_buffer, msgs_remaining, consumer):
    """ Poll consumer for messages and parse them. """
    while len(msg_buffer) < NUM_MESSAGES_BUFFER and msgs_remaining:
        msg = consumer.poll(timeout=10)
        if not msg:
            log.info('No more messages remaining')
            msgs_remaining = False
        else:
            parsed = _parse_msg(msg)
            if parsed:
                msg_buffer.append(parsed)
    return msgs_remaining


def _schedule_tasks(msg_buffer, executor, futures, task_fn, producer):
    token_bucket = LocalTokenBucket(MAX_REKOGNITION_RPS)
    recent_ids = RecentlyProcessed(NUM_RECENT_IMAGE_ID_RETENTION)
    if len(futures) < MAX_PENDING_FUTURES:
        for msg in msg_buffer:
            partial_task = partial(task_fn, msg, producer, recent_ids)
            future = executor.submit(token_bucket.throttle_fn, partial_task)
            futures.append(future)
        # Flush msg buffer after scheduling
        msg_buffer = []
    return msg_buffer


def listen(consumer, producer, task_fn):
    status_tracker = Counter({})
    msg_buffer = []
    futures = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS)
    msgs_remaining = True
    last_log = None
    while msgs_remaining:
        msgs_remaining = _poll_work(msg_buffer, msgs_remaining, consumer)
        msg_buffer = _schedule_tasks(
            msg_buffer, executor, futures, task_fn, producer
        )
        _futures, future_stats = _monitor_futures(futures)
        futures = _futures
        status_tracker += future_stats
        pending = len(futures)
        if not last_log or time.time() - last_log > 1:
            last_log = time.time()
            log.info(
                f'Stats for {task_fn}: {status_tracker}; {pending} pending'
            )
    log.info(f'Processed {status_tracker} tasks')
    log.info('No more tasks in queue. Waiting for pending tasks...')
    executor.shutdown(wait=True)
    _, future_stats = _monitor_futures(futures)
    status_tracker += future_stats
    log.info(f'Total stats: {status_tracker}')
    log.info('Worker shutting down')


if __name__ == '__main__':
    log.basicConfig(level=log.INFO, format='%(asctime)s %(message)s')
