**Note: These are the original installation instructions as provided in the udacity nanodegree Data Streaming**

# Installation instructions

These installation instructions are for running the project locally on your own device. The installation leans heavily on the usage of Docker to make all the different services available in a containerized fashion.

## Cloning the GitHub Repository

The first step to setting up your local workspace is to clone the GitHub repository:

```bash
git clone git@github.com:udacity/nd029-c2-apache-spark-and-spark-streaming-starter.git
```

## Windows Users

It is HIGHLY recommended to install the 10 October 2020 Update: https://support.microsoft.com/en-us/windows/get-the-windows-10-october-2020-update-7d20e88c-0568-483a-37bc-c3885390d212

You will then want to install the latest version of Docker on Windows: https://docs.docker.com/docker-for-windows/install/

## Using Docker for your Project

You will need to use Docker to run the project on your own computer. You can find Docker for your operating system here: https://docs.docker.com/get-docker/

It is recommended that you configure Docker to allow it to use up to 2 cores and 6 GB of your host memory for use by the course workspace. If you are running other processes using Docker simultaneously with the workspace, you should take that into account also.

The docker-compose file at the root of the repository creates 9 separate containers:

- Redis
- Zookeeper (for Kafka)
- Kafka
- Banking Simulation
- Trucking Simulation
- STEDI (Application used in Final Project)
- Kafka Connect with Redis Source Connector
- Spark Master
- Spark Worker

It also mounts your repository folder to the Spark Master and Spark Worker containers as a volume /home/workspace, making your code changes instantly available within to the containers running Spark.

Let's get these containers started!

```bash
cd [repositoryfolder]
docker-compose up
```

You should see 9 containers when you run this command:

```bash
docker ps
```