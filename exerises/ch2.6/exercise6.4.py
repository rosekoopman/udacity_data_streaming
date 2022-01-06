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


app = faust.App("exercise4", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)
popular_uris_topic = app.topic(
    "com.udacity.streams.clickevents.popular",
    key_type=str,
    value_type=ClickEvent,
)

@app.agent(clickevents_topic)
async def clickevent(clickevents):
    #
    # TODO: Filter clickevents to only those with a number higher than or
    #       equal to 100
    #       See: https://faust.readthedocs.io/en/latest/userguide/streams.html#filter-filter-values-to-omit-from-stream
    #
    #async for ...
        #
        # TODO: Send the message to the `popular_uris_topic` with a key and value.
        #
        
    async for clickevent in clickevents.filter(lambda x: x.number >= 100):
        await popular_uris_topic.send(key=clickevent.uri,
                                      value=clickevent)
    pass

if __name__ == "__main__":
    app.main()

    
# run using 
# python exercise6.4.py worker

# check using
# kafka-console-consumer --topic "com.udacity.streams.clickevents.popular" --bootstrap-server PLAINTEXT://localhost:9092