# Please complete the TODO items in the code

from dataclasses import asdict, dataclass
import json

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


@dataclass
class ClickEventSanitized(faust.Record):
    timestamp: str
    uri: str
    number: int


app = faust.App("exercise3", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents",
                              value_type=ClickEvent)

#
# TODO: Define an output topic for sanitized click events, without the user email
#
# sanitized_topic = app.topic()
sanitized_topic = app.topic("com.udacity.streams.clickevents.sanitized",
                            key_type=str,
                            value_type=ClickEventSanitized)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for clickevent in clickevents:
        #
        # TODO: Modify the incoming click event to remove the user email.
        #       Create and send a ClickEventSanitized object.
        #

        #
        # TODO: Send the data to the topic you created above.
        #       Make sure to set a key and value
        #
        
        clickevent_sanitized = ClickEventSanitized(
            timestamp=clickevent.timestamp, 
            uri=clickevent.uri, 
            number=clickevent.number)
        
        await sanitized_topic.send(
            key=clickevent.uri,
            value=clickevent_sanitized
        )

if __name__ == "__main__":
    app.main()

    
# Run the producer from faust
# python exercise6.3.py  worker

# Consume the events using kafka-console-consumer to check if production is ok
# kafka-console-consumer --bootstrap-server localhost:9092 --topic com.udacity.streams.clickevents.sanitized 