"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("Consuming weather data")
        
        value = message.value()
        temperature = value.get("temperature")
        status = value.get("status")

        if temperature is None or status  is None:
            logger.error(f"Unable to process weather due to missing value(s): temperature {temperature}, status {status}")
        
        self.temperature = temperature
        self.status = status
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #
