import worker.settings as settings
import logging as log
import asyncio
import aiohttp
import aredis
import math
import boto3
import botocore.client
from functools import partial
from collections import defaultdict
from worker.util import kafka_connect, save_thumbnail_s3
from worker.message import AsyncProducer, parse_message
from worker.image import process_image
from worker.rate_limit import RateLimitedClientSession
from worker.stats_reporting import StatsManager


class CrawlScheduler:
    """
    Watch the 'inbound_sources' Redis set for new sources to crawl. When a new
    source arrives, start listening to the '{source}_urls' topic and schedule
    them for crawling.

    Crawls are scheduled in a way that ensures cluster throughput remains high.
    The scheduler will also try to ensure that every source gets scraped
    simultaneously instead of allowing one source to dominate all scraping
    resources.
    """
    def __init__(self, kafka_client, redis, image_processor):
        self.kafka_client = kafka_client
        self.redis = redis
        self.consumers = {}
        self.image_processor = image_processor

    @staticmethod
    def _consume_n(consumer, n):
        """
        Consume N messages from a Kafka topic consumer.

        :return: A list of messages.
        """
        messages_remaining = True
        msgs = []
        while len(msgs) < n and messages_remaining:
            msg = consumer.consume(block=False)
            if msg:
                msgs.append(parse_message(msg))
            else:
                messages_remaining = False
        return msgs

    @staticmethod
    def _log_schedule_state(task_schedule):
        counts = {}
        for source in task_schedule:
            count = len(task_schedule[source])
            if count:
                counts[source] = count
        if counts:
            log.info(f'Schedule per source: {counts}')

    @staticmethod
    def _get_unfinished_tasks(task_schedule, source):
        try:
            tasks = task_schedule[source]
            return sum([not t.done() for t in tasks])
        except KeyError:
            return 0

    def _get_consumer(self, source):
        try:
            return self.consumers[source]
        except KeyError:
            consumer = self.kafka_client \
                .topics[f'{source}_urls'] \
                .get_balanced_consumer(
                    consumer_group='image_handlers',
                    auto_commit_enable=True,
                    zookeeper_connect=settings.ZOOKEEPER_HOST,
                    use_rdkafka=True
                )
            self.consumers[source] = consumer
            return consumer

    async def _schedule(self, task_schedule):
        """
        Divide available task slots proportionately between sources.

        This is a simple scheduler that prevents sources with low rate limits
        from hogging all crawl capacity. Available task slots are divided
        equally between every source.

        For a crawl with more than a few dozen sources, a new scheduler will
        be required.

        :param task_schedule: A dict mapping each source to the set of
        scheduled asyncio tasks.
        :return: A dict of messages to schedule as image resize tasks.
        """
        raw_sources = await self.redis.smembers('inbound_sources')
        sources = [str(x, 'utf-8') for x in raw_sources]
        num_sources = len(sources)
        if not num_sources:
            return {}
        # A source never gets more than 1/4th of the worker's capacity. This
        # helps prevent starvation of lower rate limit requests and ensures
        # that the first few sources to be discovered don't get all of the
        # initial task slots.
        max_share = settings.MAX_TASKS / 4
        share = min(math.floor(settings.MAX_TASKS / num_sources), max_share)
        to_schedule = {}
        for source in sources:
            num_unfinished = self._get_unfinished_tasks(task_schedule, source)
            if num_unfinished:
                log.info(f'{source} has {num_unfinished} pending tasks')
            num_to_schedule = share - num_unfinished
            consumer = self._get_consumer(source)
            source_msgs = self._consume_n(consumer, num_to_schedule)
            to_schedule[source] = source_msgs
        return to_schedule

    async def schedule_loop(self):
        """ Repeatedly schedule image processing tasks. """
        task_schedule = defaultdict(list)
        semaphore = asyncio.BoundedSemaphore(settings.MAX_TASKS)
        while True:
            to_schedule = await self._schedule(task_schedule)
            self._log_schedule_state(task_schedule)
            for source in to_schedule:
                # Cull finished tasks
                running = []
                for task in task_schedule[source]:
                    if not task.done():
                        running.append(task)
                task_schedule[source] = running
                # Add new tasks
                if to_schedule[source]:
                    log.info(f'Scheduling {len(to_schedule[source])} '
                             f'{source} downloads')
                for msg in to_schedule[source]:
                    t = asyncio.create_task(
                        self.image_processor(
                            url=msg['url'],
                            identifier=msg['uuid'],
                            source=source,
                            semaphore=semaphore,
                            attempts=msg.get('attempts', None)
                        )
                    )
                    task_schedule[source].append(t)
            await asyncio.sleep(5)


async def setup_io():
    """
    Set up all IO used by the scheduler.

    :return A list of awaitable tasks
    """
    kafka_client = kafka_connect()
    s3 = boto3.client(
        's3',
        settings.AWS_DEFAULT_REGION,
        config=botocore.client.Config(max_pool_connections=settings.MAX_TASKS)
    )
    metadata_updates = kafka_client.topics['image_metadata_updates'] \
        .get_producer(use_rdkafka=True)
    retries = kafka_client.topics['inbound_images'] \
        .get_producer(use_rdkafka=True)
    link_rot = kafka_client.topics['link_rot'].get_producer(use_rdkafka=True)
    metadata_producer = AsyncProducer(producer_topic=metadata_updates)
    retry_producer = AsyncProducer(producer_topic=retries)
    link_rot_producer = AsyncProducer(producer_topic=link_rot)
    redis_client = aredis.StrictRedis(host=settings.REDIS_HOST)
    connector = aiohttp.TCPConnector(ssl=False)
    aiosession = RateLimitedClientSession(
        aioclient=aiohttp.ClientSession(connector=connector),
        redis=redis_client
    )
    stats = StatsManager(redis_client)
    image_processor = partial(
        process_image, session=aiosession,
        persister=partial(save_thumbnail_s3, s3_client=s3),
        stats=stats,
        metadata_producer=metadata_producer,
        retry_producer=retry_producer,
        rot_producer=link_rot_producer
    )
    scheduler = CrawlScheduler(kafka_client, redis_client, image_processor)
    return (
        metadata_producer.listen(),
        retry_producer.listen(),
        link_rot_producer.listen(),
        scheduler.schedule_loop()
    )


async def listen():
    """
    Listen for image events forever.
    """
    meta_producer, retry_producer, rot_producer, scheduler = await setup_io()
    meta_producer_task = asyncio.create_task(meta_producer)
    retry_producer_task = asyncio.create_task(retry_producer)
    rot_producer_task = asyncio.create_task(rot_producer)
    scheduler_task = asyncio.create_task(scheduler)

    tasks = [
        meta_producer_task,
        retry_producer_task,
        rot_producer_task,
        scheduler_task
    ]
    await asyncio.wait(tasks)


if __name__ == '__main__':
    log.basicConfig(level=log.INFO)
    asyncio.run(listen())
