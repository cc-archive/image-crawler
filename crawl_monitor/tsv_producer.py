import json
import time
import csv
import argparse
import logging as log
from urllib.parse import urlparse
from confluent_kafka import Producer

"""
Schedules URLs for crawling from a TSV file.
"""
log.basicConfig(level=log.INFO)


def _parse_row(row):
    """ Read a row from the TSV and encode it as a message. """
    parsed = {
        'url': _add_protocol(row['url']),
        'uuid': row['identifier'],
        'source': row['source']
    }
    return bytes(json.dumps(parsed), 'utf-8')


def _add_protocol(url: str):
    parsed = urlparse(url)
    if parsed.scheme == '':
        return 'http://' + url
    else:
        return url


parser = argparse.ArgumentParser(
    description='Schedule a crawl from a TSV file.'
)
parser.add_argument(
    'tsv_path', metavar='f', type=str, nargs=1, help='A TSV file'
)
parser.add_argument(
    'kafka_hosts', metavar='h', type=str, nargs=1,
    help='A comma separated list of Kafka hosts, e.g. 127.0.0.1:9092.'
)
parsed_args = parser.parse_args()
tsv_path = parsed_args.tsv_path[0]
in_tsv = open(tsv_path, 'r')
hosts = parsed_args.kafka_hosts[0]
log.info(f'Connecting to Kafka broker(s): {hosts}')
reader = csv.DictReader(in_tsv, delimiter='\t')
start = time.monotonic()
log.info('Beginning production of messages')
producer = Producer({'bootstrap.servers': hosts})
count = 0
for idx, row in enumerate(reader):
    count = idx
    encoded = _parse_row(row)
    while True:
        try:
            producer.produce('inbound_images', encoded)
        except BufferError:
            # Give the producer time to catch up before retrying
            producer.poll(1)
        break
    if idx % 10000 == 0:
        log.info(f'Produced {idx} messages so far')
print(f'Produced {count} at rate {count / (time.monotonic() - start)}/s')
in_tsv.close()
