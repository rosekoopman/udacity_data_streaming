**Note: These are the original installation instructions as provided in the udacity nanodegree Data Streaming**

# Installation instructions

These installation instructions are for running the project locally on your own device. The installation leans heavily on the usage of Docker to make all the different services available in a containerized fashion.

## Prerequisites

The following are required to complete this project:

* Docker
* Python 3.7
* Access to a computer with a minimum of 16gb+ RAM and a 4-core CPU to execute the simulation

## Assumptions

These instructions assume that you have already installed Docker and Docker Compose on your computer. If Docker and Docker Compose are not installed, please refer to these links for installation -

- (Docker Installation)[https://docs.docker.com/engine/install/]
- (Docker Compose Installation)[https://docs.docker.com/compose/install/]
  
> **_NOTE:_**  Windows Users - You must use Windows Subsystem for Linux (WSL) 2. This guide is written against the Ubuntu 20.04 distribution. These instructions should work for any Linux installation on Windows. However, other Linux distributions have not been tested and are not supported. For best results, install and use Ubuntu 20.04.

> **_NOTE:_**  Note - Windows Users must execute the commands inside of the Ubuntu 20.04 environment. Running the following commands from Powershell or the Windows command prompt will not work.

## Running Dependencies with Docker Compose

This project makes use of docker-compose for running your project’s dependencies:

- Kafka
- Zookeeper
- Schema Registry
- REST Proxy
- Kafka Connect
- KSQL
- Kafka Connect UI
- Kafka Topics UI
- Schema Registry UI
- Postgres

The docker-compose file **does not run your code**.

To start docker-compose, navigate to the starter directory containing `docker-compose.yaml` and run the following commands:

> **_NOTE:_**  Note - Windows Users must execute this command inside of the Ubuntu 20.04 environment

    $> cd starter
    $> docker-compose up

    Starting zookeeper          ... done
    Starting kafka0             ... done
    Starting schema-registry    ... done
    Starting rest-proxy         ... done
    Starting connect            ... done
    Starting ksql               ... done
    Starting connect-ui         ... done
    Starting topics-ui          ... done
    Starting schema-registry-ui ... done
    Starting postgres           ... done

You will see a large amount of text print out in your terminal and continue to scroll. This is normal! This means your dependencies are up and running.

To check the status of your environment, you may run the following command at any time from a separate terminal instance:

    $> docker-compose ps

                Name                          Command              State                     Ports
    -----------------------------------------------------------------------------------------------------------------
    starter_connect-ui_1           /run.sh                         Up      8000/tcp, 0.0.0.0:8084->8084/tcp
    starter_connect_1              /etc/confluent/docker/run       Up      0.0.0.0:8083->8083/tcp, 9092/tcp
    starter_kafka0_1               /etc/confluent/docker/run       Up      0.0.0.0:9092->9092/tcp
    starter_ksql_1                 /etc/confluent/docker/run       Up      0.0.0.0:8088->8088/tcp
    starter_postgres_1             docker-entrypoint.sh postgres   Up      0.0.0.0:5432->5432/tcp
    starter_rest-proxy_1           /etc/confluent/docker/run       Up      0.0.0.0:8082->8082/tcp
    starter_schema-registry-ui_1   /run.sh                         Up      8000/tcp, 0.0.0.0:8086->8086/tcp
    starter_schema-registry_1      /etc/confluent/docker/run       Up      0.0.0.0:8081->8081/tcp
    starter_topics-ui_1            /run.sh                         Up      8000/tcp, 0.0.0.0:8085->8085/tcp
    starter_zookeeper_1            /etc/confluent/docker/run       Up      0.0.0.0:2181->2181/tcp, 2888/tcp, 3888/tcp

## Connecting to Services in Docker Compose

Now that your project’s dependencies are running in Docker Compose, we’re ready to get our project up and running.

> **_NOTE:_**  Windows Users Only: You must first install librdkafka-dev in your WSL Linux. Run the following command in your Ubuntu terminal:
        ```sudo apt-get install librdkafka-dev -y```

Now you should be able to follow all the instructions on the Project Directions page and complete your project.

## Stopping Docker Compose and Cleaning Up

When you are ready to stop Docker Compose you can run the following command:

    $> docker-compose stop
    Stopping starter_postgres_1           ... done
    Stopping starter_schema-registry-ui_1 ... done
    Stopping starter_topics-ui_1          ... done
    Stopping starter_connect-ui_1         ... done
    Stopping starter_ksql_1               ... done
    Stopping starter_connect_1            ... done
    Stopping starter_rest-proxy_1         ... done
    Stopping starter_schema-registry_1    ... done
    Stopping starter_kafka0_1             ... done
    Stopping starter_zookeeper_1          ... done

If you would like to clean up the containers to reclaim disk space, as well as the volumes containing your data:

    $> docker-compose rm -v
    Going to remove starter_postgres_1, starter_schema-registry-ui_1, starter_topics-ui_1, starter_connect-ui_1, starter_ksql_1, starter_connect_1, starter_rest-proxy_1, starter_schema-registry_1, starter_kafka0_1, starter_zookeeper_1
    Are you sure? [yN] y
    Removing starter_postgres_1           ... done
    Removing starter_schema-registry-ui_1 ... done
    Removing starter_topics-ui_1          ... done
    Removing starter_connect-ui_1         ... done
    Removing starter_ksql_1               ... done
    Removing starter_connect_1            ... done
    Removing starter_rest-proxy_1         ... done
    Removing starter_schema-registry_1    ... done
    Removing starter_kafka0_1             ... done
    Removing starter_zookeeper_1          ... done
