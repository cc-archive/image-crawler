import boto3
import enum


class TaskStatus(enum.Enum):
    SUCCEEDED = 1
    IGNORED_DUPLICATE = 2
    ERROR = 3


def handle_image_task(message, recent_ids):
    """
    Get Rekognition labels for an image and output results to a Kafka topic.

    Only call Rekognition if we haven't recently processed the ID.
    """
    image_uuid = message
    if recent_ids.seen_recently(image_uuid):
        # We don't want to send the same image to Rekognition multiple times.
        return TaskStatus.IGNORED_DUPLICATE
    boto3_session = boto3.session.Session()
    response = detect_labels_query(image_uuid, boto3_session)
    return TaskStatus.SUCCEEDED, response


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
