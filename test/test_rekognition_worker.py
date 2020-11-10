import time
import logging as log
import pytest
import botocore
import uuid
from analysis.rekognition_worker import (
    listen
)
from analysis.task import TaskStatus
from analysis.util import LocalTokenBucket, RecentlyProcessed
from test.mocks import FakeConsumer, FakeProducer

log.basicConfig(level=log.INFO, format='%(asctime)s %(message)s')


def make_mock_msg():
    _uuid = uuid.uuid4()
    return f'{{"identifier":"{_uuid}"}}'


def mock_work_function(*args, **kwargs):
    time.sleep(1)
    recent_ids = args[1]
    if recent_ids.seen_recently(args[0]):
        return TaskStatus.IGNORED_DUPLICATE
    return TaskStatus.SUCCEEDED, 'mock event output'


def mock_work_fn_failure(*args, **kwargs):
    raise ValueError()


def mock_boto3_fn_failure(*args, **kwargs):
    raise botocore.exceptions.ClientError('test')


def test_scheduler_terminates():
    consumer = FakeConsumer()
    producer = FakeProducer()
    fake_events = [make_mock_msg() for _ in range(100)]
    for fake_event in fake_events:
        consumer.insert(fake_event)
    listen(consumer, producer, mock_work_function)


def test_end_to_end():
    consumer = FakeConsumer()
    producer = FakeProducer()
    fake_events = [make_mock_msg() for _ in range(90)]
    dupe_uuid = uuid.uuid4()
    dupes = [f'{{"identifier":"{dupe_uuid}"}}' for _ in range(10)]
    fake_events.extend(dupes)
    for fake_event in fake_events:
        consumer.insert(fake_event)
    results = listen(consumer, producer, mock_work_function)
    assert results[TaskStatus.SUCCEEDED] == 91
    assert results[TaskStatus.IGNORED_DUPLICATE] == 9


def test_exception_raised():
    """ Make sure exceptions in child threads get caught """
    with pytest.raises(ValueError):
        consumer1 = FakeConsumer()
        consumer2 = FakeConsumer()
        producer = FakeProducer()
        fake_events = [make_mock_msg() for _ in range(100)]
        for fake_event in fake_events:
            consumer1.insert(fake_event)
            consumer2.insert(fake_event)
        listen(consumer1, producer, mock_work_fn_failure)
        listen(consumer2, producer, mock_boto3_fn_failure)


def test_token_bucket_contention():
    token_bucket = LocalTokenBucket(2)
    should_acquire_1 = token_bucket._acquire_token()
    should_acquire_2 = token_bucket._acquire_token()
    should_not_acquire = token_bucket._acquire_token()
    assert should_acquire_1
    assert should_acquire_2
    assert not should_not_acquire


def test_token_bucket_refresh():
    refresh_rate = 0.01
    token_bucket = LocalTokenBucket(1, refresh_rate_sec=refresh_rate)
    token_acquired = token_bucket._acquire_token()
    time.sleep(refresh_rate)
    token_acquired_2 = token_bucket._acquire_token()
    assert token_acquired
    assert token_acquired_2


def test_recently_seen():
    _id = uuid.uuid4()
    recent_ids = RecentlyProcessed(retention_num=2)
    first_time_seen = recent_ids.seen_recently(_id)
    second_time_seen = recent_ids.seen_recently(_id)
    assert not first_time_seen
    assert second_time_seen


def test_recently_seen_deletion():
    _id = uuid.uuid4()
    _id2 = uuid.uuid4()
    recent_ids = RecentlyProcessed(retention_num=2)
    recent_ids.seen_recently(_id)
    recent_ids.seen_recently(uuid.uuid4())
    recent_ids.seen_recently(_id2)
    forgotten = recent_ids.seen_recently(_id)
    should_remember = recent_ids.seen_recently(_id2)
    assert not forgotten
    assert should_remember
