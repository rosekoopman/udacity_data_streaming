# Please complete the TODO items in the code

from dataclasses import asdict, dataclass
from datetime import timedelta
import json
import random

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int


app = faust.App("exercise8", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)

#
# TODO: Define a hopping window of 1 minute with a 10-second step
#       See: https://faust.readthedocs.io/en/latest/reference/faust.tables.table.html?highlight=hopping#faust.tables.table.Table.hopping
#
uri_summary_table = app.Table("uri_summary", default=int).hopping(size=timedelta(minutes=1), step=timedelta(seconds=10), expires=timedelta(minutes=2))


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for ce in clickevents.group_by(ClickEvent.uri):
        
        # uri_summary_table[ce.uri] += ce.number
        uri_summary_table[ce.uri] += 1
        
        print(f"[current] {ce.uri}: {uri_summary_table[ce.uri].current()}")
        print(f"[now    ] {ce.uri}: {uri_summary_table[ce.uri].now()}")
        print(f"[value  ] {ce.uri}: {uri_summary_table[ce.uri].value()}")


if __name__ == "__main__":
    app.main()

    
# python exercise6.8.py  worker

# In this table, table[k].now() returns the most recent value for the current processing window, overriding the _relative_to_ option used to create the window.

# In this table, table[k].current() returns the most recent value relative to the time of the currently processing event, overriding the _relative_to_ option used to create the window.

# In this table, table[k].value() returns the most recent value relative to the time of the currently processing event, and is the default behavior.

# Note to myself:
# now() and value() give identical results. When not expiration is set currect() always gives 0, if an expiration is set then the result is identical to now() and value().
# maybe differences between now, current, value only occur when you "make the current value relative to the current local time, relative to a different field in the event (if it has a custom timestamp field), or of another event."?

