import asyncio
import aredis
import aiohttp
import logging as log
import crawl_monitor.settings as settings
from crawl_monitor.rate_limit import rate_limit_regulator
from crawl_monitor.structured_logging import log_state
from crawl_monitor.source_splitter import SourceSplitter
from confluent_kafka import Producer, Consumer
from multiprocessing import Process
from multiprocessing_logging import install_mp_handler


async def monitor():
    session = aiohttp.ClientSession()
    redis = aredis.StrictRedis(host=settings.REDIS_HOST)
    # For sharing information between rate limit regulator and monitoring system
    info = {}
    regulator = asyncio.create_task(rate_limit_regulator(session, redis, info))
    structured_logger = asyncio.create_task(log_state(redis, info))
    await asyncio.wait([regulator, structured_logger])


def run_splitter():
    """
    Takes messages from the inbound_images topic and divides each source
    into its own queue for scheduling.
    """
    inbound_images = Consumer({
        'bootstrap.servers': settings.KAFKA_HOSTS,
        'group.id': 'splitter',
        'auto.offset.reset': 'earliest'
    })
    producer = Producer({'bootstrap.servers': settings.KAFKA_HOSTS})
    inbound_images.subscribe(['inbound_images'])
    splitter = SourceSplitter(producer, inbound_images)
    splitter.split()


if __name__ == '__main__':
    log.basicConfig(level=log.INFO)
    install_mp_handler()
    p = Process(target=run_splitter)
    p.start()
    asyncio.run(monitor())
