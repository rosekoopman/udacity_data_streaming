# Please complete TODO items in the code

from dataclasses import asdict, dataclass, field
import json
import time
import random

import requests
from confluent_kafka import avro, Consumer, Producer
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient
from faker import Faker


faker = Faker()
REST_PROXY_URL = "http://localhost:8082"

AVRO_SCHEMA = """{
    "type": "record",
    "name": "click_event",
    "fields": [
        {"name": "email", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "uri", "type": "string"},
        {"name": "number", "type": "int"}
    ]
}"""


def produce():
    """Produces data using REST Proxy"""

    # TODO: Set the appropriate headers
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#content-types
    headers = {"Content-Type" : "application/vnd.kafka.avro.v2+json"}
    # TODO: Update the below payload to include the Avro Schema string
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name)
    data = {
        "value_schema": AVRO_SCHEMA,
        "records": [{"value": asdict(ClickEvent())}]
    }
    # lesson4.exercise6.click_events
    resp = requests.post(
        f"{REST_PROXY_URL}/topics/ex4.6",  # TODO
        data=json.dumps(data),
        headers=headers,
    )

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to send data to REST Proxy {json.dumps(resp.json(), indent=2)}")

    print(f"Sent data to REST Proxy {json.dumps(resp.json(), indent=2)}")


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))


def main():
    """Runs the simulation against REST Proxy"""
    try:
        while True:
            produce()
            time.sleep(0.5)
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()

### checks

# note: new topic is created automatically! 

# check topic creation
# kafka-topics --list --zookeeper localhost:2181

# data that data is produced to topic
# kafka-console-consumer --topic ex4.6 --bootstrap-server localhost:9092

# note AVRO_SCHEMA is already is string.
# In case avro-schema would be defined as dictionary, you still need to convert is to string.
# This can be done with
# json.dumps(avro_schema)

# note if an key_schema is defined, than this also needs to be provided at the 
# time of the produce (post), inside the data object line 37-40. Similar to value_schema
# a parameter key_schema needs to be added. Both value_schema and key_schema need to 
# be strings (json.dumps!).