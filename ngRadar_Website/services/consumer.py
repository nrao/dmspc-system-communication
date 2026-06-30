import hashlib
import json
import os
from datetime import datetime
from confluent_kafka import Consumer
from dotenv import load_dotenv
import ast
from ngRadar_Website.enums import Stations
import threading
from ngRadar_Website.models.models import ObservatoryEvent
from django.db import close_old_connections


load_dotenv()  # loads .env from current working dir

class ConsumerService:

    def __init__(self):
        self.thread = None
        self.running = False

        self.config = {
            "bootstrap.servers": os.getenv("BOOTSTRAP_SERVER"),
            "fetch.max.bytes": 8388608,
            "session.timeout.ms": 45000,
            "client.id": "universal-consumer",
            "group.id": "consumer-group",
            "auto.offset.reset": "earliest",
          }

        self.topic = ["GBT_data", "DSOC_data"]  #NOTE The topic which the messages will be received from, rename accordingly to whatever topic you are using


    def latency_calc(self, event_time):
      #calculates the latency of the message from the time it was sent to the time it was received
      #returns latency in milliseconds

      event_time = datetime.strptime(event_time, "%Y-%m-%d %H:%M:%S.%f")
      current_time = datetime.now()
      latency = current_time - event_time
      latency_ms = latency.total_seconds() * 1000
      return latency_ms

    def DB_columns(self, value):
      #dissects the payload to get individual values, and publishes to the correct column in the database
      #some values only exist for messages with images - these have if statements

      # for the below if-else logic, is scanning for the source the best approach?
      # what about scanning for if "Tx_WF" in value or "Rec_WF" in value?
      # Something like:
          # if "Tx_WF" in value:
          #   # GBT-style payload?
          # else:
          #   # Image-style payload?
      # How we have it currently is fine and works fine, just something 
      # to consider in case we need to decipher by payload type rather
      # than by the station. Is GBT the ONLY station ever sending Tx_Wf
      # and Rec_WF? Is it more important first to know "who" is sending it or
      # what type of payload is being sent? 


      # Passing a dict instead in the event that the payload schema grows in size in the future
      # And then only updating the dict depending on the 2 diff payloads we have right now
      station = Stations[value["Source"]]
      event_time = value["Timestamp"]

      data = {
              "object_id": value["Object_ID"],
              "target": value["Object"],
              "station": station,
              "event_time": event_time,
              "latency_ms": self.latency_calc(event_time),
      }

      if station == Stations.GBT:
        data.update({
              "product_type": None,
              "product_id": None,
              "created_at": None,
              "xmit_station": Stations.GBT,
              "rcvr_station": None,  # not specified in GBT-data.csv
              "image_file": None,
              "num_bytes": None,
              "rec_waveform": value["Rec_WF"],
              "tx_waveform": value["Tx_WF"],
        })
      # Image payload
      # Are the GBT and DSOC messages describing the same observation, just from different systems? 
      # In other words, is GBT always the transmitter for every DSOC image, or do we expect future scenarios where 
      # another station could transmit? 
      # That would determine whether hardcoding Stations.GBT as the transmitter is the right long-term approach.
      else:
        data.update({
            "product_type": value["Type"],
            "product_id": value["Image_ID"],
            "created_at": datetime.now(),
            "station": station,
            "xmit_station": Stations.GBT, # xmit station not specified in DSOC-data.csv, will the xmitter always be GBT here?
            "rcvr_station": station,
            "image_file": ast.literal_eval(value["Image"]),
            "num_bytes": value["Bytes"],
            "rec_waveform": None,
            "tx_waveform": None,
        })
      
      return data


    
    def publish_DB(self, **data):
      try:
          close_old_connections()

          ObservatoryEvent.objects.create(**data)

          print("Payload saved to database successfully.")

      except Exception as e:
          print(f"Database error: {e}")
    
    
    
    def log_messages(self, data):
       print(
            f"Received message from {data['station']} "
            f"for object {data['target']} "
            f"(Object ID: {data['object_id']})."
        )
       
       if data["station"] == Stations.GBT:
          if data["tx_waveform"] == "Tx_OFF":
              print("Transmitter is currently OFF.")
          else:
              print(f"Observing with waveform {data['tx_waveform']}.")
       else:
          print("Checking if quick-look product is ready...")

          if data["product_type"] not in ("Spec", "DDM"):
              print("Image is of an unknown type. Expecting 'Spec' or 'DDM'.")
          else:
              image_name = (
                  "CW Spectrum plot"
                  if data["product_type"] == "Spec"
                  else "DDM"
              )
              print(
                  f"{image_name} is ready "
                  f"(Image ID: {data['product_id']}). "
                  f"Produced by {data['station']}."
              )

              unique = hashlib.sha256(
                  str(data["image_file"]).encode("utf-8")
              ).hexdigest()

              filename = (
                  f"{data['product_type']}-"
                  f"{data['product_id']}-"
                  f"{data['event_time']}-"
                  f"{unique[:15]}.png"
              )

              print(f"Image saved as {filename}")
       


    def consume(self):
      #creates a new kafka consumer instance
      consumer = Consumer(self.config)
      #subscribes to the specified kafka topic
      consumer.subscribe(self.topic)

      try:
        while self.running:
          #consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0) #polls for messages for 1 second
          #if msg is not None and msg.error() is None:
            if msg is None:
                continue
            if msg.error() is not None:
                print("Consumer error:", msg.error())
                continue

            value = json.loads(msg.value().decode("utf-8"))

            data = self.DB_columns(value)
            self.publish_DB(**data)
            self.log_messages(data)

      except KeyboardInterrupt: 
        pass
      finally:
        #closes the consumer connection
        consumer.close()



    def start(self):
        if self.thread is None or not self.thread.is_alive():
            self.running = True
            self.thread = threading.Thread(
                target=self.consume,
                daemon=True
            )
            self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)



consumer_service = None

def get_consumer_service():
  global consumer_service

  if consumer_service is None:
      consumer_service = ConsumerService()

  return consumer_service

