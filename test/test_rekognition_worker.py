import time
import logging as log
import pytest
from analysis.rekognition_worker import listen, LocalTokenBucket
from test.mocks import FakeConsumer, FakeProducer

log.basicConfig(level=log.INFO, format='%(asctime)s %(message)s')


def mock_work_function(*args, **kwargs):
    time.sleep(1)


def mock_work_fn_failure(*args, **kwargs):
    raise ValueError()


def test_scheduler_terminates():
    consumer = FakeConsumer()
    producer = FakeProducer()
    fake_events = ["1"] * 100
    for fake_event in fake_events:
        consumer.insert(fake_event)
    listen(consumer, producer, mock_work_function)


def test_exception_raised():
    """ Make sure exceptions in child threads get caught """
    with pytest.raises(ValueError):
        consumer = FakeConsumer()
        producer = FakeProducer()
        fake_events = ["1"] * 100
        for fake_event in fake_events:
            consumer.insert(fake_event)
        listen(consumer, producer, mock_work_fn_failure)


def test_token_bucket_contention():
    token_bucket = LocalTokenBucket(2)
    should_acquire_1 = token_bucket.acquire_token()
    should_acquire_2 = token_bucket.acquire_token()
    should_not_acquire = token_bucket.acquire_token()
    assert should_acquire_1
    assert should_acquire_2
    assert not should_not_acquire


def test_token_bucket_refresh():
    refresh_rate = 0.01
    token_bucket = LocalTokenBucket(1, refresh_rate_sec=refresh_rate)
    token_acquired = token_bucket.acquire_token()
    time.sleep(refresh_rate)
    token_acquired_2 = token_bucket.acquire_token()
    assert token_acquired
    assert token_acquired_2

