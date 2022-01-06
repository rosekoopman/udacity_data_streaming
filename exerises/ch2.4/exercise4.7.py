# Please complete TODO items in this code

import asyncio
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
TOPIC_NAME = "ex4.7"
CONSUMER_NAME = "ex4.7-consumer"
CONSUMER_GROUP = f"{CONSUMER_NAME}-group-{random.randint(0,10000)}"


async def consume():
    """Consumes from REST Proxy"""
    
    #### part 1: Create an instance (consumer?) in a consumer group ####
    
    # TODO: Define a consumer name
    # consumer_name = ""
    # TODO: Define the appropriate headers
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#content-types
    headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
    # TODO: Define the consumer group creation payload, use avro
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#post--consumers-(string-group_name)
    data = {"name": CONSUMER_NAME,
            "format": "avro"}
    resp = requests.post(
        f"{REST_PROXY_URL}/consumers/{CONSUMER_GROUP}",
        data=json.dumps(data),
        headers=headers,
    )
    try:
        resp.raise_for_status()
    except:
        print(
            f"Failed to create REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
        )
        return
    print("REST Proxy consumer group created")

    resp_data = resp.json()
    
    print(f"base_uri = {resp_data['base_uri']}")
    # prints: base_uri = http://localhost:8082/consumers/ex4.7-consumer-group-527/instances/ex4.7-consumer
    
    #### part 2: Subscribe the consumer (in consumer group) to a list of topic(s) ####

    #
    # TODO: Create the subscription payload
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#consumers
    #
    data = {"topics": [TOPIC_NAME]}
    resp = requests.post(
        f"{resp_data['base_uri']}/subscription", data=json.dumps(data), headers=headers
    )
    try:
        resp.raise_for_status()
    except:
        print(
            f"Failed to subscribe REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
        )
        return
    print("REST Proxy consumer subscription created")
    
    #### part 3: Get records from the topic(s) using the consumer that has been subscribed to the topic ####

    while True:
        #
        # TODO: Set the Accept header to the same data type as the consumer was created with
        #       See: https://docs.confluent.io/current/kafka-rest/api.html#get--consumers-(string-group_name)-instances-(string-instance)-records
        #
        headers = {"Accept": "application/vnd.kafka.avro.v2+json"}
        
        resp = requests.get(f"{resp_data['base_uri']}/records?timeout=10000", headers=headers)
        try:
            resp.raise_for_status()
        except:
            print(
                f"Failed to fetch records with REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
            )
            return
        print("Consumed records via REST Proxy:")
        print(f"{json.dumps(resp.json())}")
        await asyncio.sleep(0.1)


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))

    schema = avro.loads(
        """{
        "type": "record",
        "name": "click_event",
        "namespace": "com.udacity.lesson3.exercise2",
        "fields": [
            {"name": "email", "type": "string"},
            {"name": "timestamp", "type": "string"},
            {"name": "uri", "type": "string"},
            {"name": "number", "type": "int"}
        ]
    }"""
    )


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = AvroProducer(
        {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "schema.registry.url": "http://localhost:8081",
        }
    )
    try:
        while True:
            p.produce(
                topic=topic_name,
                value=asdict(ClickEvent()),
                value_schema=ClickEvent.schema,
            )
            await asyncio.sleep(0.1)
    except:
        raise


async def produce_consume(topic_name):
    """Runs the Producer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume())
    await t1
    await t2


def main():
    """Runs the simulation against REST Proxy"""
    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()

    
### Clean up

## Delete consumers using kafka commandline tool

# list consumer groups
# kafka-consumer-groups  --list --bootstrap-server localhost:9092
# list members of a group
# kafka-consumer-groups --describe --group ex4.7-consumer-group-6930 --members --bootstrap-server localhost:9092
# describe group
# kafka-consumer-groups --describe --group ex4.7-consumer-group-4878 --bootstrap-server localhost:9092
# delete consumer group
# kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group ex4.7-consumer-group-6930
