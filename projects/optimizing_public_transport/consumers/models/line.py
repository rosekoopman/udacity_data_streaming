"""Contains functionality related to Lines"""
import json
import logging

from models import Station


logger = logging.getLogger(__name__)


class Line:
    """Defines the Line Model"""

    def __init__(self, color):
        """Creates a line"""
        self.color = color
        self.color_code = "0xFFFFFF"
        if self.color == "blue":
            self.color_code = "#1E90FF"
        elif self.color == "red":
            self.color_code = "#DC143C"
        elif self.color == "green":
            self.color_code = "#32CD32"
        self.stations = {}

    def _handle_station(self, value):
        """Adds the station to this Line's data model"""
        if value["line"] != self.color:
            return
        self.stations[value["station_id"]] = Station.from_message(value)
        # try:
        #     logger.info(f" --- --- --- ---  Known stations for line {self.color}: {list(self.stations.keys())}")
        # except:
        #     logger.info('failed to print stations')

    def _handle_arrival(self, message):
        """Updates train locations"""
        # try:
        #     logger.info(f" --- --- --- ---  Known stations for line {self.color}: {list(self.stations.keys())}")
        # except:
        #     logger.info('failed to print stations')
        value = message.value()
        prev_station_id = value.get("prev_station_id")
        prev_dir = value.get("prev_direction")
        if prev_dir is not None and prev_station_id is not None:
            prev_station = self.stations.get(prev_station_id)
            if prev_station is not None:
                prev_station.handle_departure(prev_dir)
            else:
                logger.warning("Unable to handle previous station due to missing station")
        else:
            logger.warning(
                "Unable to handle previous station due to missing previous info"
            )
        station_id = value.get("station_id")
        station = self.stations.get(station_id)
        if station is None:
            logger.warning("Unable to handle message due to missing station")
            logger.warning(f"Station_id {station_id}")
            logger.warning(f"Available stations: {list(self.stations.keys())}")
            return
        station.handle_arrival(
            value.get("direction"), value.get("train_id"), value.get("train_status")
        )

    def process_message(self, message):
        """Given a kafka message, extract data"""
        logger.info(f"[{message.topic()}] Processing message ")
        if message.topic() == "org.chicago.cta.stations.table.v1": # Set the conditional correctly to the stations Faust Table
            try:
                value = json.loads(message.value())
                self._handle_station(value)
            except Exception as e:
                logger.fatal("bad station? %s, %s", value, e)
        elif "org.chicago.cta.station.arrivals" in message.topic(): # Set the conditional to the arrival topic
            self._handle_arrival(message)
        elif message.topic() == "TURNSTILE_SUMMARY": # Set the conditional to the KSQL Turnstile Summary Topic
            json_data = json.loads(message.value())
            station_id = json_data.get("STATION_ID")
            station = self.stations.get(station_id)
            if station is None:
                logger.info(f"Unable to handle message: line {self.color} does not stop at station {station_id}")
                return
            station.process_message(json_data)
        else:
            logger.warning(
                "Unable to find handler for message from topic %s", message.topic
            )
