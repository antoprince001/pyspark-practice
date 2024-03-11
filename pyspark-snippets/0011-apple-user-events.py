# Count the number of user events performed by MacBookPro users.
# Output the result along with the event name.
# Sort the result based on the event count in the descending order.

import pyspark

playbook_events = playbook_events.where(playbook_events.device == 'macbook pro').groupby(playbook_events.event_name).count()

playbook_events.toPandas()

