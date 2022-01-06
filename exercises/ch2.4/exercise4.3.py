# Please complete the TODO items in this code

import asyncio
import json

import requests


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "clicks-jdbc"


def configure_connector():
    """Calls Kafka Connect to create the Connector"""
    print("creating or updating kafka connect connector...")

    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        return

    #
    # TODO: Complete the Kafka Connect Config below for a JDBC source connector.
    #       You should whitelist the `clicks` table, use incrementing mode and the
    #       incrementing column name should be id.
    #
    #       See: https://docs.confluent.io/current/connect/references/restapi.html
    #       See: https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/source_config_options.html
    #
    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,  # TODO
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",  # TODO
                    "topic.prefix": "connect-",  # TODO
                    "mode": "incrementing",  # TODO
                    "incrementing.column.name": "id",  # TODO
                    "table.whitelist": "clicks",  # TODO
                    "tasks.max": 1,
                    "connection.url": "jdbc:postgresql://localhost:5432/classroom",
                    "connection.user": "root",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                },
            }
        ),
    )

    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        print(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    print("connector created successfully.")
    print("Use kafka-console-consumer and kafka-topics to see data!")


if __name__ == "__main__":
    configure_connector()
    
    
# to run:
# python exercise4.3.py

### check if data has been loaded into kafka correctly

# check status of new connector
# curl http://localhost:8083/connectors/clicks-jdbc/status | python -m json.tool

# check status of task of new connector
# curl http://localhost:8083/connectors/clicks-jdbc/tasks/0/status | python -m json.tool

# check logs
# tail -f /var/log/journal/confluent-kafka-connect.service.log 

# check if new topic exists
# kafka-topics --list --zookeeper localhost:2181

# check content of topic connect-clicks
# kafka-console-consumer --topic connect-clicks --bootstrap-server localhost:9092 --from-beginning
