import json
from confluent_kafka import Producer
from stations import rcvr
import base64


def read_metadata():
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

  # serialize to JSON string (UTF-8)
  key_bytes = rcvr_DSN.encode("utf-8")
  value_bytes = metadata

  JSON_Tuple= (key_bytes, value_bytes)
  
  return JSON_Tuple


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

def readDMMImage():
  try:
        imageFile=open("mtex-ngVLA.png", "rb")
        print("Opening Image")
  except IOError:
      print("An error occurred while trying to read the file")
      imageInBytes = "Error"
  except:
      print("An unknown error occurred")
      imageInBytes = "Error"
  else:
      imageInBytes = imageFile.read()
  
  return imageInBytes
  

def produce(topic, config, key, value, imageInBytes):
  # creates a new producer instance
  producer = Producer(config)

  JSON_Image = base64.b64encode(imageInBytes).decode('utf-8')

  messageDataDict = {"metadata": value, "imageData": JSON_Image}

  messageData = json.dumps(messageDataDict).encode("utf-8")
  # produces a sample message
  producer.produce(topic, key=key, value=messageData)
  print(f"Produced message to topic {topic}: key = {key} value = {value}")

  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()


def main():
  topic = "DDM_Practice1"  # NOTE The topic to which the messages will be sent, rename accordingly

  JSON_Tuple = read_metadata()

  key_bytes = JSON_Tuple[0]

  value_bytes = JSON_Tuple[1]
  
  config = read_config()

  imageInBytes = readDMMImage()

  produce(topic, config, key_bytes, value_bytes, imageInBytes)


main()
