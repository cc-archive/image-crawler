import botocore.exceptions
import logging as log
import time
import concurrent.futures
import enum
import worker.settings as settings
from functools import partial
from collections import defaultdict, Counter
from confluent_kafka import Consumer, Producer
from analysis.task import handle_image_task
from analysis.util import LocalTokenBucket, RecentlyProcessed, parse_msg

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


class TaskStatus(enum.Enum):
    SUCCEEDED = 1
    IGNORED_DUPLICATE = 2
    ERROR = 3


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


def _poll_work(msg_buffer, msgs_remaining, consumer):
    """ Poll consumer for messages and parse them. """
    while len(msg_buffer) < NUM_MESSAGES_BUFFER and msgs_remaining:
        msg = consumer.poll(timeout=10)
        if not msg:
            log.info('No more messages remaining')
            msgs_remaining = False
        else:
            parsed = parse_msg(msg)
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
        pending_futures, future_stats = _monitor_futures(futures)
        futures = pending_futures
        status_tracker += future_stats
        pending = len(futures)
        if not last_log or time.time() - last_log > 1:
            last_log = time.time()
            log.info(
                f'{task_fn}: {status_tracker}; {pending} pending'
            )
    log.info(f'Processed {status_tracker} tasks')
    log.info('No more tasks in queue. Waiting for pending tasks...')
    executor.shutdown(wait=True)
    _, future_stats = _monitor_futures(futures)
    status_tracker += future_stats
    log.info(f'Aggregate stats: {status_tracker}')
    log.info('Worker shutting down')


if __name__ == '__main__':
    log.basicConfig(level=log.INFO, format='%(asctime)s %(message)s')
    output_producer = Producer({'bootstrap.servers': settings.KAFKA_HOSTS})
    inbound_consumer = Consumer({
        'bootstrap.servers': settings.KAFKA_HOSTS,
        'group.id': 'image_metadata_updates',
        'auto.offset.reset': 'earliest'
    })
    inbound_consumer.subscribe([])
    listen(inbound_consumer, output_producer, handle_image_task)
