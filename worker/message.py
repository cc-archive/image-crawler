import asyncio
import json
import logging as log
import time
import wand.image
import datetime as dt
from wand.exceptions import WandException

from PIL import Image


class AsyncProducer:
    """
    When we scrape an image, we often times want to collect additional
    information about it (such as the resolution) and incorporate it into our
    database. This is accomplished by encoding the discovered metadata into
    a Kafka message and publishing it into the corresponding topic.

    pykafka is not asyncio-friendly, so we need to batch our messages together
    and intermittently send them to Kafka synchronously. Launch
    `MetadataProducer.listen` as an asyncio task to do this.
    """
    def __init__(self, producer_topic, frequency=60):
        """
        :param producer_topic: A pykafka producer.
        :param frequency: How often to publish queued events.
        """
        self.frequency = frequency
        self.producer = producer_topic
        self._messages = []

    def enqueue_message(self, msg: dict):
        try:
            _msg_json = json.dumps(msg)
            _msg = bytes(_msg_json, 'utf-8')
        except TypeError:
            log.warning(f'Failed to encode message: {msg}')
            return
        self._messages.append(_msg)

    async def listen(self):
        """ Intermittently publish queued events to Kafka. """
        while True:
            queue_size = len(self._messages)
            if queue_size:
                log.info(f'Publishing {queue_size} events')
                start = time.monotonic()
                for msg in self._messages:
                    self.producer.produce(msg)
                rate = queue_size / (time.monotonic() - start)
                self._messages = []
                log.info(f'publish_rate={rate}/s')
            await asyncio.sleep(self.frequency)


def parse_message(message):
    decoded = json.loads(str(message.value, 'utf-8'))
    return decoded


def notify_quality(img: Image, buffer, identifier, metadata_producer):
    """ Collect quality metadata. """
    height, width = img.size
    filesize = buffer.getbuffer().nbytes
    buffer.seek(0)
    try:
        compression_quality = wand.image.Image(file=buffer).compression_quality
    except WandException:
        compression_quality = None
    metadata_producer.enqueue_message(
        {
            'height': height,
            'width': width,
            'identifier': identifier,
            'filesize': filesize,
            'compression_quality': compression_quality
        }
    )


def notify_exif(img: Image, identifier, metadata_producer):
    if 'exif' in img.info:
        exif = {hex(k): v for k, v in img.getexif().items()}
        if exif:
            metadata_producer.enqueue_message(
                {
                    'identifier': identifier,
                    'exif': exif
                }
            )


def notify_retry(identifier, source, url, attempts, retry_producer):
    retry_producer.enqueue_message(
        {
            'url': url,
            'uuid': identifier,
            'source': source,
            'attempts': attempts
        }
    )


def notify_404(identifier, link_rot_producer):
    link_rot_producer.enqueue_message(
        {
            'identifier': identifier,
            'timestamp': dt.datetime.utcnow().isoformat()
        }
    )
