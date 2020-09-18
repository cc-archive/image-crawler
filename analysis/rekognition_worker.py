import boto3
import botocore.exceptions
import logging as log
import time
import threading
import json
import concurrent.futures
from functools import partial

IMG_BUCKET = 'cc-image-analysis'
LABELS_TOPIC = 'image_analysis_labels'
# Used to respect AWS service limits
MAX_REKOGNITION_RPS = 50
# Number of pending messages to store simultaneously in memory
NUM_MESSAGES_BUFFER = 1500
MAX_PENDING_FUTURES = 1000
# These are network-bound threads, it's OK to use a lot of them
NUM_THREADS = 50


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
        token_acquired = False
        while not token_acquired:
            token_acquired = self._acquire_token()
            time.sleep(0.01)
        task_to_throttle()


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


def handle_image_task(message, output_producer):
    """
    Get Rekognition labels for an image and output results to a Kafka topic
    """
    msg = json.loads(message)
    image_uuid = msg['identifier']
    boto3_session = boto3.session.Session()
    response = detect_labels_query(image_uuid, boto3_session)
    enqueue(response, output_producer)


def _monitor_futures(futures):
    """ Summarizes task progress and handles exceptions """
    _futures = []
    finished = 0
    for f in futures:
        if f.done():
            finished += 1
        try:
            f.result()
        except botocore.exceptions.ClientError:
            log.warning("Boto3 failure: ", exc_info=True)
        else:
            # Keep pending futures
            _futures.append(f)
    return _futures, finished


def _poll_work(msg_buffer, msgs_remaining, consumer):
    while len(msg_buffer) < NUM_MESSAGES_BUFFER and msgs_remaining:
        msg = consumer.poll(timeout=10)
        if not msg:
            log.info('No more messages remaining')
            msgs_remaining = False
        else:
            msg_buffer.append(msg)
    return msgs_remaining


def _schedule_tasks(msg_buffer, executor, futures, task_fn, producer):
    token_bucket = LocalTokenBucket(MAX_REKOGNITION_RPS)
    if len(futures) < MAX_PENDING_FUTURES:
        for msg in msg_buffer:
            partial_task = partial(task_fn, str(msg.value(), 'utf-8'), producer)
            future = executor.submit(token_bucket.throttle_fn, partial_task)
            futures.append(future)
        # Flush msg buffer after scheduling
        msg_buffer = []
    return msg_buffer


def listen(consumer, producer, task_fn):
    finished_tasks = 0
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
        _futures, completed_futures = _monitor_futures(futures)
        futures = _futures
        finished_tasks += completed_futures
        pending = len(futures)
        if not last_log or time.time() - last_log > 1:
            last_log = time.time()
            log.info(
                f'Completed {finished_tasks} {task_fn} calls; {pending} pending'
            )
    log.info(f'Processed {finished_tasks} tasks')
    log.info('No more tasks in queue. Waiting for pending tasks...')
    executor.shutdown(wait=True)
    _monitor_futures(futures)
    log.info('Worker shutting down')


if __name__ == '__main__':
    log.basicConfig(level=log.INFO, format='%(asctime)s %(message)s')

