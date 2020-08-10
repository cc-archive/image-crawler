import json
import logging as log
import redis
import crawl_monitor.settings as settings

NUM_SPLIT = 'num_split'
SOURCE_SET = 'inbound_sources'


def parse_message(message):
    try:
        decoded = json.loads(str(message.value(), 'utf-8'))
        decoded['source'] = decoded['source'].lower()
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
        log.error(f'Failed to parse message {message}', exc_info=True)
        return None
    return decoded


class SourceSplitter:
    """
    Split URLs into different Kafka topics by source for scheduling purposes.
    Intended to be run in a dedicated process.

    Example input:

    topic: inbound_urls
    {source: 'example1', uuid: xxxxxx, 'url': "xxxxx.org}
    {source: 'example2', uuid: xxxxxx, 'url': "xxxxx.org}

    Output:
    topic: urls.example1:
    {uuid: xxxxxx, 'url': "xxxxx.org}
    topic: urls.example1:
    {uuid: xxxxxx, 'url': "xxxxx.org}

    Each time a new topic is created, the name of the source gets
    put into the 'inbound_sources' set in Redis.
    """

    def __init__(self, producer, consumer):
        # Map source to producer topic
        self.producer = producer
        self.sources = set()
        self.consumer = consumer

    def split(self):
        redis_client = redis.StrictRedis(settings.REDIS_HOST)
        log.info('Starting splitter')
        while True:
            msg_count = 0
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            parsed = parse_message(msg)
            if parsed:
                source = parsed['source']
                if source not in self.sources:
                    redis_client.sadd(SOURCE_SET, source)
                    self.sources.add(source)
                del parsed['source']
                encoded_msg = bytes(json.dumps(parsed), 'utf-8')
                while True:
                    try:
                        self.producer.produce(f'urls.{source}', encoded_msg)
                    except BufferError:
                        self.producer.poll(1)
                    msg_count += 1
                    break
            if msg_count % 1000 == 0:
                redis_client.incrby(NUM_SPLIT, 1000)
