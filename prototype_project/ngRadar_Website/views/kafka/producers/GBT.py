import json
import time
import csv
from confluent_kafka import Producer


topic = "GBT_data"  # NOTE The topic to which the messages will be sent, rename accordingly to whatever topic you want to send the DDM payloads to.

run = False # set to true when ready to produce to kafka

def read_config():
  # reads the client (producer) configuration from producer.properties
  # and returns it as a key-value map
  config = {}
  with open("producer.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config


def produce(topic, config, key, value):
  # creates a new producer instance
  producer = Producer(config)

  # producing a message to the specified topic 
  producer.produce(topic, key=key, value=value) 
  print(f"Produced message to topic {topic} with key {key}.")

  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()


with open("GBT-data.csv", newline="") as f:
    reader = csv.DictReader(f, delimiter=" ")  # uses header row as keys
    for row in reader:
        # build JSON payload from the row
        payload = {
            "Object": row["Object"],
            "Object_ID": row["Object_ID"],
            "Source": row["Source"],
            "Tx_Status": row["Tx_Status"],
            "Transmitted_WF": row["Transmitted_WF"],
            "Recorded_WF": row["Recorded_WF"],
            "Timestamp": f"{time.time()}"
        }
        print(payload) # sanity check

        key = str(payload["Object_ID"]) if payload.get("Object_ID") else None
        value = json.dumps(payload).encode("utf-8")

        if run:
            config = read_config()
            produce(topic, config, key, value)
            time.sleep(10)
