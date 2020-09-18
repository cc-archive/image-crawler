import boto3
import botocore.exceptions
import logging as log
import time
import threading
import json
import concurrent.futures

IMG_BUCKET = 'cc-image-analysis'
LABELS_TOPIC = 'image_analysis_labels'
# Used to respect AWS service limits
MAX_REKOGNITION_RPS = 50
# Number of pending messages to store simultaneously in memory
NUM_MESSAGES_BUFFER = 1500
MAX_PENDING_FUTURES = 1000
# These are IO bound threads, it's OK to use a lot of them
NUM_THREADS = 50


class LocalTokenBucket:
    def __init__(self, max_val, refresh_rate_sec=1):
        self._refresh_rate_sec = refresh_rate_sec
        self._lock = threading.Lock()
        self._last_timestamp = None
        self._MAX_TOKENS = max_val
        self._curr_tokens = max_val

    def acquire_token(self):
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


def task(image_uuid, output_producer, token_bucket):
    token_acquired = False
    while not token_acquired:
        token_acquired = token_bucket.acquire_token()
        time.sleep(0.01)
    try:
        boto3_session = boto3.session.Session()
        response = detect_labels_query(image_uuid, boto3_session)
        enqueue(response, output_producer)
    except botocore.exceptions:
        log.error('Boto3 failure: ', exc_info=True)


def monitor_futures(futures):
    _futures = []
    finished = 0
    for f in futures:
        if f.done():
            finished += 1
            if f.exception():
                raise f.exception()
        else:
            # Keep pending futures
            _futures.append(f)
    return _futures, finished


def listen(consumer, producer, task_fn):
    processed = 0
    token_bucket = LocalTokenBucket(MAX_REKOGNITION_RPS)
    msg_buffer = []
    futures = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS)
    msgs_remaining = True
    last_log = None
    while msgs_remaining:
        # Get work from the queue
        while len(msg_buffer) < NUM_MESSAGES_BUFFER and msgs_remaining:
            msg = consumer.poll(timeout=10)
            if not msg:
                log.info('No more messages remaining')
                msgs_remaining = False
            else:
                msg_buffer.append(msg)
        # Schedule tasks
        if len(futures) < MAX_PENDING_FUTURES:
            for msg in msg_buffer:
                future = executor.submit(
                    task_fn, str(msg.value(), 'utf-8'), producer, token_bucket
                )
                futures.append(future)
            msg_buffer = []
        # Summarize current progress and trim finished tasks
        _futures, completed = monitor_futures(futures)
        futures = _futures
        processed += completed
        pending = len(futures)
        if not last_log or time.time() - last_log > 1:
            last_log = time.time()
            log.info(f'Completed {processed} {task_fn} calls')
    log.info(f'Processed {processed} tasks; {pending} pending')
    log.info('No more tasks in queue. Waiting for pending tasks...')
    executor.shutdown(wait=True)
    monitor_futures(futures)
    log.info('Worker shutting down')


if __name__ == '__main__':
    log.basicConfig(level=log.INFO, format='%(asctime)s %(message)s')

