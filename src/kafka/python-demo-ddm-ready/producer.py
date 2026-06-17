import json
from confluent_kafka import Producer
from stations import rcvr

topic = "DDM_Ready"  # NOTE The topic to which the messages will be sent, rename accordingly

with open('metadata.json', 'r') as file:
  metadata = json.load(file)

# checking to see if valid recevier station is provided in the metadata file,
# then extract DNS to use as the key to guarantee ordering by partition.
receiver = metadata["rcvr_station"]
if receiver not in [station[0] for station in rcvr.values()]:
    raise ValueError(f"Receiver station {receiver} not valid. Message not sent.")
 # this is a bit of a convoluted way to get the DSN number for the receiver station, but it works.
 # It looks up the receiver station in the rcvr dictionary and gets the corresponding DSN number.
rcvr_DSN = rcvr[[key for key, value in rcvr.items() if value[0] == receiver][0]][1]


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


# serialize to JSON string (UTF-8)
key_bytes = rcvr_DSN.encode("utf-8")
value_bytes = json.dumps(metadata).encode("utf-8")


def produce(topic, config, key, value):
  # creates a new producer instance
  producer = Producer(config)

  # produces a sample message
  producer.produce(topic, key=key, value=value)
  print(f"Produced message to topic {topic}: key = {key} value = {value}")

  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()


def main():
  config = read_config()

  produce(topic, config, key_bytes, value_bytes)


main()
