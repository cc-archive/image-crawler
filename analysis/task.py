import json
import boto3
import enum

LABELS_TOPIC = 'image_analysis_labels'


class TaskStatus(enum.Enum):
    SUCCEEDED = 1
    IGNORED_DUPLICATE = 2
    ERROR = 3


def handle_image_task(message, output_producer, recent_ids):
    """
    Get Rekognition labels for an image and output results to a Kafka topic.

    Only call Rekognition if we haven't recently processed the ID.
    """
    msg = json.loads(message)
    image_uuid = msg['identifier']
    if recent_ids.seen_recently(image_uuid):
        # We don't want to send the same image to Rekognition multiple times.
        return TaskStatus.IGNORED_DUPLICATE
    boto3_session = boto3.session.Session()
    response = detect_labels_query(image_uuid, boto3_session)
    enqueue(response, output_producer)
    return TaskStatus.SUCCEEDED


def enqueue(rekognition_response: dict, kafka_producer):
    resp_json = json.dumps(rekognition_response).encode('utf-8')
    kafka_producer.produce(LABELS_TOPIC, resp_json)


def detect_labels_query(image_uuid, boto3_session):
    img = {
        'S3Object': {
            'Bucket': 'cc-image-analysis',
            'Name': f'{image_uuid}.jpg'
        }
    }
    rekog_client = boto3_session.client('rekognition')
    response = rekog_client.detect_labels(Image=img)
    data = {
        'image_uuid': image_uuid,
        'response': response
    }
    return data
