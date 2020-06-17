import json
import time
import csv
import argparse
import logging as log
from pykafka import KafkaClient

"""
Schedules URLs for crawling from a TSV file.
"""


def _parse_row(row):
    """ Read a row from the TSV and encode it as a message. """
    parsed = {
        'url': row['url'],
        'uuid': row['identifier'],
        'source': row['source']
    }
    return bytes(json.dumps(parsed), 'utf-8')


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
tsv_path = parsed_args.tsv_path
in_tsv = open(tsv_path, 'r')
client = KafkaClient(hosts=parsed_args.kafka_hosts)
topic = client.topics['inbound_images']
reader = csv.DictReader(in_tsv, delimiter='\t')
start = time.monotonic()
with topic.get_producer(
        use_rdkafka=True,
        max_queued_messages=5000000,
        block_on_queue_full=True) as producer:
    for idx, row in enumerate(reader):
        encoded = _parse_row(row)
        producer.produce(encoded)
        if idx % 10000 == 0:
            log.info(f'Produced {idx} messages so far')
print(f'Produced {idx} at rate {idx / (time.monotonic() - start)}/s')
in_tsv.close()
