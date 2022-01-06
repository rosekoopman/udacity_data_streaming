"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

# Define app and input and output topics. 
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1, value_type=TransformedStation)

# Creat a Faust table to construct. Its content will be saved in the output topic
table = app.Table(
   "org.chicago.cta.stations.table.v1",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


# Clean the events of the input topic and store the result in the Faust table
@app.agent(topic)
async def clean_stations(stations):
    async for station in stations:

        line = "blue" * station.blue + "red" * station.red + "green" * station.green

        # if len(line) == 0:
        #     logger.warning(f"According to the postgress data NO line stops at station {station.station_id}. Manually setting line to 'green'")
        #     line = "green"

        transformed_station = TransformedStation(station.station_id,
                                                station.station_name,
                                                station.order,
                                                line)

        table[station.station_id] = transformed_station

        logger.info(f"Cleaned {station.station_id} : {table[station.station_id]}")



if __name__ == "__main__":
    app.main()


# To run:
# faust -A faust_stream worker -l info

# Note: In the lectures we ran faust code as 
# python faust_stream.py worker
# but for some reason we need to do it differently now