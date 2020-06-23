import asyncio
import aiohttp
import logging as log
from functools import partial
from io import BytesIO
from PIL import Image, UnidentifiedImageError
from worker import settings as settings
from worker.message import notify_quality, notify_exif, notify_retry, notify_404
from worker.stats_reporting import StatsManager

NO_RATE_TOKEN = 'NoRateToken'
SERVER_DISCONNECTED = 'ServerDisconnected'

RETRY_CODES = {
    400,
    401,
    403,
    429,
    500,
    NO_RATE_TOKEN,
    SERVER_DISCONNECTED
}

MAX_RETRIES = 1


async def _handle_error(
        retry_producer, rot_producer, stats, identifier, source, url,
        err_code=None, attempts=None
):
    if attempts is None:
        attempts = 0
    attempts_remaining = attempts < MAX_RETRIES
    await stats.record_error(source, code=err_code)
    if err_code in RETRY_CODES and attempts_remaining and retry_producer:
        log.info(f'Retrying {url} later')
        attempts += 1
        notify_retry(identifier, source, url, attempts, retry_producer)
    elif err_code == 404 and rot_producer:
        notify_404(identifier, rot_producer)


async def process_image(
        persister, session, url, identifier, stats: StatsManager, source,
        semaphore, metadata_producer=None, retry_producer=None,
        rot_producer=None, attempts=None
):
    """
    Get an image, collect dimensions metadata, thumbnail it, and persist it.
    :param stats: A StatsManager for recording task statuses.
    :param source: Used to determine rate limit policy. Example: flickr, behance
    :param semaphore: Limits concurrent execution of process_image tasks
    :param identifier: Our identifier for the image at the URL.
    :param persister: The function defining image persistence. It
    should do something like save an image to disk, or upload it to
    S3.
    :param session: An aiohttp client session.
    :param url: The URL of the image.
    :param metadata_producer: The outbound message queue for dimensions
    metadata.
    :param retry_producer: The outbound message queue for download retries.
    :param rot_producer: The outbound message queue for detecting link rot.
    :param attempts: The number of times we have tried and failed to download
    the image.
    """
    async with semaphore:
        loop = asyncio.get_event_loop()
        report_err = partial(
            _handle_error, retry_producer, rot_producer, stats, identifier,
            source, url, attempts=attempts
        )
        try:
            img_resp = await session.get(url, source)
        except aiohttp.client_exceptions.ServerDisconnectedError:
            await report_err(err_code='ServerDisconnected')
            return
        if not img_resp:
            await report_err(err_code='NoRateToken')
            return
        elif img_resp.status >= 400:
            await report_err(err_code=img_resp.status)
            return
        buffer = BytesIO(await img_resp.read())
        try:
            img = await loop.run_in_executor(None, partial(Image.open, buffer))
            if metadata_producer:
                notify_quality(img, buffer, identifier, metadata_producer)
                notify_exif(img, identifier, metadata_producer)
        except UnidentifiedImageError:
            await report_err(err_code='UnidentifiedImageError')
            return
        thumb = await loop.run_in_executor(
            None, partial(thumbnail_image, img)
        )
        await loop.run_in_executor(
            None, partial(persister, img=thumb, identifier=identifier)
        )
        await stats.record_success(source)


def thumbnail_image(img: Image):
    img.thumbnail(size=settings.TARGET_RESOLUTION, resample=Image.NEAREST)
    output = BytesIO()
    if img.mode != 'RGB':
        img = img.convert('RGB')
    img.save(output, format='JPEG', quality=50)
    output.seek(0)
    return output
