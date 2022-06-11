from typing import Callable

from google.cloud import pubsub_v1
import os
from concurrent import futures
from google.cloud import pubsub_v1
# TODO(developer)
os.environ[
'GOOGLE_APPLICATION_CREDENTIALS'] = "/home/ninosha/Desktop/projects/achisproeqti/key.json"


def message(request_type, data):
    data = str(data).encode()
    return request_type, data

def create_push(project_id, topic_id, data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    published = publisher.publish(topic_path, message="k", data=b'data')
    print(published.result())


project_id = "bitcoindata-352508"
topic_id = "crimes"
subscription_id = "al-sub"
endpoint = "https://europe-west1-bitcoindata-352508.cloudfunctions.net/crime_api_bgq"

create_push(project_id, topic_id, 234)


