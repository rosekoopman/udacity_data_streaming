# Please complete the TODO items in the code.

import asyncio
from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker


faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "org.udacity.exercise3.4.purchases"


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        """Serializes the object in JSON string format"""
        # TODO: Serializer the Purchase object
        #       See: https://docs.python.org/3/library/json.html#json.dumps
        return json.dumps({"username": self.username,
                           "currency": self.currency,
                           "amount": self.amount})


async def produce_sync(topic_name):
    """Produces data synchronously into the Kafka Topic"""
#     p = Producer({"bootstrap.servers": BROKER_URL})

    p = Producer({"bootstrap.servers": BROKER_URL,
                  "linger.ms": 100,                      # default=5. Number of ms to wait before batch of messages is send by producer to broker
                  "batch.num.messages": 20,              # default=10000. Number of messages to collect in batch before batch is send by producer to broker
                  "queue.buffering.max.messages": 2,     # default=100000. Maximum queue size measured in number of messages. 
                  "queue.buffering.max.kbytes": 65536    # default=1048576. Maximum queue size measured in kbytes. 
                 })
    
    # Note: I had expected it to crash because of the small queue size but it didn't. I do not understand why! Also, setting linger.ms to 1000 or larger 
    # does not yield any results. I also do not understand what happens here.
    
    # TODO: Write a synchronous production loop.
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.flush
    while True:
        # TODO: Instantiate a `Purchase` on every iteration. Make sure to serialize it before
        #       sending it to Kafka!
        
        p.produce(topic_name, Purchase().serialize())
        p.flush()
        
        # Do not delete this!
        await asyncio.sleep(0.01)

        
def main():
    """Checks for topic and creates the topic if it does not exist"""
    create_topic(TOPIC_NAME)
    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("shutting down")

    
async def produce_consume():
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce_sync(TOPIC_NAME))
    t2 = asyncio.create_task(_consume(TOPIC_NAME))
    await t1
    await t2

    
async def _consume(topic_name):
    """Consumes produced messages"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])
    num_consumed=0
    while True:
        msg = c.consume(timeout=0.001)
        if msg:
            num_consumed += 1
            if num_consumed % 100 == 0:
                print(f"consumed {num_consumed} messages")
        else:
            await asyncio.sleep(0.01)

        
def create_topic(client):
    """Creates the topic with the given topic name"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    futures = client.create_topics(
        [NewTopic(topic=TOPIC_NAME, num_partitions=5, replication_factor=1)]
    )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            print("exiting production loop")


if __name__ == "__main__":
    main()