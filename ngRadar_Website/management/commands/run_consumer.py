import time
import os
from django.core.management.base import BaseCommand
from django.conf import settings
from ngRadar_Website.models.models import ObservatoryEvent # will need to store parsed data to db
from enum import Stations # can use enum for stations

"""
This has not been tested at all.
This command listens to a Kafka topic, parses the message, and stores it in a PostgreSQL database.
"""


class Command(BaseCommand):
    help = 'Processes Kafka telemetry, calculates latency, saves images and logs to Postgres'

        # mock_producer is going to put attachments in media/ddm_payloads/ since we are using
        # that for local object storage rather than a cloud storage solution. So we need the
        # consumer to look for those objects there. Maybe, unless someone
        # has a better idea for how to handle the image files locally.


        self.stdout.write(self.style.SUCCESS("Connected to Kafka cluster! Listening for messages..."))

            # 1. Latency calculation?
            # Should calculate latency here or in the view? Right not the latency graphy logic is in
            # ngRadar_Website/latency-graph.py but I am unsure where that logic should be implemented.

             # 2. Extract Metadata from message headers
                # The following pseudocode kind of ecplains how I think we could do this? 
                # idk it's past midnight lmao.
                
                # If header_key.source is "GBT":
                #     parse GBT metadata
                #     store in db table
                # else if header_key.source is "DSOC":
                #     parse DSOC metadata
                #     read raw image payload bytes straight out of the message value field
                #     store in db table


# need to do more research on how Django management commands work, is triggered, and how it 
# would be "listening" for new kafka messages at a specified frequency.
            


#==================================
# Nicole's consumer logic here:
#==================================

# import hashlib
# import json
# import os
# import csv
# import time
# from PIL import Image
# from confluent_kafka import Consumer


# topic = "DDM_Payload"   # NOTE The topic which the messages will be received from, rename accordingly to whatever topic you are using
# #consumer_group = "gbt-group"  # NOTE rename to whatever consumer group name you want to use

# '''
# stats_file = "ddm_stats.csv"


# def append_stats(num_bytes, latency_ms,filename=None):
#   """Append DDM payload statistics for later analysis."""
  
#   fieldnames = ["filename", "num_bytes", "latency_ms"]
#   file_exists = os.path.exists(stats_file)
#   with open(stats_file, "a", newline="") as fh:
#     writer = csv.DictWriter(fh, fieldnames=fieldnames)
#     if not file_exists:
#       writer.writeheader()
#     writer.writerow({
#       "filename": filename or "",
#       "num_bytes": num_bytes,
#       "latency_ms": f"{latency_ms:.2f}"
#     })
# '''

# def read_config():
#   # reads the client (consumer) configuration from consumer.properties
#   # and returns it as a key-value map
#   config = {}
#   with open("consumer.properties") as fh:
#     for line in fh:
#       line = line.strip()
#       if len(line) != 0 and line[0] != "#":
#         parameter, value = line.strip().split('=', 1)
#         config[parameter] = value.strip()
#   return config


# def consume(topic, config):
#   # creates a new consumer instance
#   consumer = Consumer(config)

#   # subscribes to the specified topic
#   consumer.subscribe([topic])

#   try:
#     while True:
#       # consumer polls the topic and prints any incoming messages
#       msg = consumer.poll(1.0) # polls for messages for 1 second
#       if msg is not None and msg.error() is None:
#         key = msg.key().decode("utf-8")
#         value = msg.value()
#         headers = dict(msg.headers() or []) # 
#         meta = json.loads(headers.get("DDM metadata", b"{}").decode("utf-8")) # loading the DDM metadata from the message headers, which is stored as a JSON string. We decode it from bytes to a string and then load it into a dictionary so we can access the individual fields.

#         digest = hashlib.sha256(value).hexdigest()
#         filename = f"received-DDM-{digest:.15}.png" # creating a unique filename for the received DDM payload using a random UUID. You can change this to whatever naming convention you want.
        
#         with open(filename, 'wb') as file: # writing the received DDM payload to a file in bytes format.
#           file.write(value)

#         send_time = float(meta.get('message_timestamp')) # extract timestamp from header

#         latency_ms = (time.time() - send_time)*1000 #calculate latency in ms

#         print(f"Saved DDM for obs_id '{meta.get('obs_id')}' as {filename} ({len(value)} bytes). Image created at {meta.get('created_timestamp')} by {meta.get('source')}. Latency: {latency_ms:.2f} ms") 

#         #append_stats(
#           #num_bytes=len(value),
#           #latency_ms=latency_ms,
#           #filename=filename)

#         # open and display the consumed image
#         #img = Image.open(filename)
#         #img.show()

#   except KeyboardInterrupt: 
#     pass
#   finally:
#     # closes the consumer connection
#     consumer.close()


# def main():
#   config = read_config()

#   consume(topic, config)


# main()
