import os
from datetime import datetime, timedelta
from PIL import Image
from django.core.management.base import BaseCommand
from confluent_kafka import Consumer
from dotenv import load_dotenv
import random
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import io
from ngRadar_Website.models.models import gbtEvent, dsocEvent
# from django.db import close_old_connections

"""
This code will:
- consume a message from the GBT
- create an image file
- load the image data + the uuid into the DB
"""

load_dotenv()  # loads .env from current working dir


config = {
    "bootstrap.servers": os.environ["BOOTSTRAP_SERVER"],
    "fetch.max.bytes": 8388608,
    "session.timeout.ms": 45000,
    "client.id": "universal-consumer",
    "group.id": "consumer-group",
    "auto.offset.reset": "earliest",
  }


topic = ["GBT_data"]  #consumes from the GBT's topic

''' We aren't currently calculating latency at this step
We have to decide how to implement latency in addition to having a simulated delay

def latency_calc(event_time):
  #calculates the latency of the message from the time it was sent to the time it was received
  #returns latency in milliseconds

  event_time = datetime.strptime(event_time, "%Y-%m-%d %H:%M:%S.%f")
  current_time = datetime.now()
  latency = current_time - event_time
  latency_ms = latency.total_seconds() * 1000
  return latency_ms
'''



def DB_import(uuid):
    
  gbt_data = gbtEvent.objects.filter(uuid=uuid).values_list('uuid', 'target', 'tx_waveform', 'event_time', 'latency_ms').first()

  return gbt_data




def DB_columns():
  #defines the column values specific to DSOC/images

    station = random.choice([9, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99])  #randomly selects a station from the list of stations

    data = {
        "product_type": "DDM",
        "product_id": f"DDM{random.randint(1000,9999)}",
        "station": station,
        "created_at": datetime.now() - timedelta(seconds=10),
        "xmit_station": "GBT",
        "rcvr_station": station,
    }

    return data


def publish_DB(image_key, num_bytes, data):
  #saves the image path to the database
  data.update({
      "image_key": image_key,
      "num_bytes": num_bytes
  })

  try:
          close_old_connections()
          # close_old_connections()

          dsocEvent.objects.create(**data)
          msg = "Payload saved to database successfully."

  except Exception as e:
          msg = f"Database error: {e}"

  return print(msg)


def create_img(tx_waveform):
    #generate a random image payload to simulate the DSOC's DDM product: 
    matplotlib.use('Agg')  # Use a non-interactive backend for matplotlib
        
    #generating random data and formatting the graph:
    x_data = np.random.uniform(-30, 30, 20)
    y_data = np.random.uniform(-300, 300, 20)

    plt.scatter(x_data, y_data)
    plt.axhline(0, color='black', linewidth=0.5)
    plt.axvline(0, color='black', linewidth=0.5)
    plt.title(f"DDM for {tx_waveform}")
    plt.xlabel("Doppler Freq (Hz)")
    plt.ylabel("Range (km)")
    plt.grid(True)

    #saving the bytes to a buffer instead of a file
    byte_buffer = io.BytesIO()
    plt.savefig(byte_buffer, format='png')
    byte_buffer.seek(0)

    image_file = byte_buffer.getvalue()

    plt.close()  # Close the plot to free memory
        
    num_bytes = len(image_file)

    return image_file, num_bytes

def save_image_to_seaweedfs(target, image_file, uuid):
    # Save the image to SeaweedFS using S3 API
    import boto3
    from botocore.exceptions import NoCredentialsError

    s3 = boto3.client(
        's3',
        endpoint_url=os.environ.get('WEED_S3_DOMAIN'),
        aws_access_key_id=os.environ.get('WEED_S3_ACCESS_KEY'),
        aws_secret_access_key=os.environ.get('WEED_S3_SECRET_KEY')
    )

    image_key = f"ddm/{target}/{uuid}.png"

    try:
        s3.put_object(Bucket=os.environ.get('WEED_S3_BUCKET'), Key=image_key, Body=image_file)
        print(f"Image saved to SeaweedFS at {image_key}")
        image_url = (
            f"{os.environ['WEED_S3_DOMAIN']}/"
            f"{os.environ['WEED_S3_BUCKET']}/"
            f"{image_key}"
        )
    except NoCredentialsError:
        print("Credentials not available for SeaweedFS S3.")
    
    return image_key


def consume(topic, config):
  #creates a new consumer instance
  consumer = Consumer(config)

  #subscribes to the specified topic
  consumer.subscribe(topic)

  try:
    while True:
      #consumer polls the topic and prints any incoming messages
        msg = consumer.poll(1.0) #polls for messages for 1 second
      #if msg is not None and msg.error() is None:
        if msg is None:
            continue
        if msg.error() is not None:
            print("Consumer error:", msg.error())
            continue

        #decode the GBT payload that is a single string of just the uuid:
        uuid = msg.key().decode("utf-8")

        #use the uuid from the payload to import the correct line of data from the GBT table:
        gbt_data = DB_import(uuid)

        #create the rest of the column values specific to DSOC/images:
        data = DB_columns()

        uuid, target, tx_waveform, event_time, latency_ms = gbt_data
        image_file, num_bytes = create_img(tx_waveform)

        image_key = save_image_to_seaweedfs(gbt_data.target, image_file, dsocEvent.uuid)

        publish_DB(image_key, num_bytes, data)

        print(f"Received message from {data.station}; DDM is ready in SeaweedFS (Image Path: {data.image_key}).")

        ''' Previous code using old functions:
        #create the rest of the column values specific to DSOC/images:
        product_type, product_id, station,created_at, xmit_station, rcvr_station = DB_columns()

        publish_DB(uuid, product_type, product_id, station, created_at, xmit_station, rcvr_station, image_file, num_bytes)
        '''
        #unique = hashlib.sha256(str(value['Image']).encode('utf-8')).hexdigest()
        #filename = f"{value['Type']}-{value['Image_ID']}-{value['Timestamp']}-{unique:.15}.png"
        #print(f"Image saved as {filename}")


  except KeyboardInterrupt: 
    pass
  finally:
    #closes the consumer connection
    consumer.close()


class Command(BaseCommand):
    help = "Runs the DSOC simulator"

    def handle(self, *args, **options):
        print("Starting DSOC simulator")

        consume(topic, config)