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
CONSUMER_GROUP = f"{CONSUMER_NAME}-group-9498"

async def delete_consumer():
    print('Deleting consumer group')
    headers = {"Content-Type": "application/vnd.kafka.v2+json"}
    resp = requests.delete(f"{REST_PROXY_URL}/consumers/{CONSUMER_GROUP}/instances/{CONSUMER_NAME}",
                          headers=headers)
    
    try:
        resp.raise_for_status()
        print('response:')
        print(resp)
    except:
        print('Failed to delete:')
        resp_data = resp.json()
        print(resp_data)
    
async def unsubscribe_consumer():
    print('Unsubscribe consumer group')
    headers = {"Accept": "application/vnd.kafka.json.v2+json"}
    resp = requests.delete(f"{REST_PROXY_URL}/consumers/{CONSUMER_GROUP}/instances/{CONSUMER_NAME}/subscription",
                          headers=headers)
    try:
        resp.raise_for_status()
        print('response:')
        print(resp)
    except:
        print('Failed to unsubscibe:')
        resp_data = resp.json()
        print(resp_data)

async def produce_consume(topic_name):
    """Runs the Producer tasks"""
    t1 = asyncio.create_task(unsubscribe_consumer())
    t2 = asyncio.create_task(delete_consumer())
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

## using REST API

# I do not manage to delete the entire group!

## Note:

# after the python job exercise4.7.py has been cancelled the consumer instance within the consumer group disappears automatically. 
# I seems like it is not necessary to unsubscribe!