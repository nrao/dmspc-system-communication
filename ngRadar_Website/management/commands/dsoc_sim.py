import os
from datetime import datetime, timedelta, timezone
from PIL import Image
import uuid
from django.core.management.base import BaseCommand
from confluent_kafka import Consumer
from dotenv import load_dotenv
import random
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import io
from ngRadar_Website.models.models import gbtEvent, dsocEvent
from pathlib import Path
from ngRadar_Website.enums import Stations
import time
from botocore.exceptions import EndpointConnectionError


"""
This code will:
- consume a message from the GBT
- create an image file
- save image file to seaweedfs object store
- load the image key + the uuid into the DB
"""

load_dotenv()  # Load environment variables from .env file

p = Path("../../../../out/ngrok_endpoint.env")
text = p.read_text().strip()

bootstrap = None
for line in text.splitlines():
    if line.startswith("BOOTSTRAP_SERVER="):
        bootstrap = line.split("=", 1)[1].strip()
        break

if not bootstrap:
    raise RuntimeError("BOOTSTRAP_SERVER not found in /out/ngrok_endpoint.env")

config = {
    "bootstrap.servers": bootstrap,
    "fetch.max.bytes": 8388608,
    "session.timeout.ms": 45000,
    "client.id": "dsoc-consumer",
    "group.id": "consumer-group",
    "auto.offset.reset": "earliest",
  }


topic = ["GBT_data"]  #consumes from the GBT's topic

def latency_calc(event_time):
  #calculates the latency of the message from the time it was sent to the time it was received
  #returns latency in milliseconds

  current_time = datetime.now(timezone.utc)
  latency = current_time - event_time
  latency_ms = latency.total_seconds() * 1000
  return latency_ms


def DB_import(uuid):
    
  gbt_data = gbtEvent.objects.filter(uuid=uuid).values_list('object_id', 'target', 'tx_waveform', 'event_time').first()

  return gbt_data


def DB_columns(gbt_data):
    data = {
        "event_time": datetime.now(timezone.utc),
        "object_id": gbt_data[0], # object_id
        "target": gbt_data[1],    # target
    }

    return data


def publish_DB(image_key, num_bytes, data):
    # Copy the dictionary so we don't accidentally mutate data out-of-scope
    # since we are not using class based methods
    payload_data = data.copy()
    payload_data.update({
        "image_key": image_key,
        "num_bytes": num_bytes
    })

    try:
        # Create and capture the instantiated record model
        time.sleep(3)
        record = dsocEvent.objects.create(**payload_data)
        print("Payload saved to database successfully.")
        return record  # <-- Return the actual object record

    except Exception as e:
        print(f"Database error: {e}")
        return None  # <-- Return None if something broke


def create_img(tx_waveform):
    #generate a random image payload to simulate the DSOC's DDM product: 
    matplotlib.use('Agg')  # Use a non-interactive backend for matplotlib
        
    #generating random data and formatting the graph:
    x_data = np.random.uniform(-30, 30, 40)
    y_data = np.random.uniform(-300, 300, 40)

    plt.scatter(x_data, y_data, color='red')
    plt.axhline(0, color='black', linewidth=0.5)
    plt.axvline(0, color='black', linewidth=0.5)
    plt.title(f"DDM for {tx_waveform}", size=20)
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

def save_image_to_seaweedfs(target, image_file, dsoc_uuid):
    # Save the image to SeaweedFS using S3 API
    import boto3
    from botocore.exceptions import NoCredentialsError

    s3 = boto3.client(
        's3',
        endpoint_url=os.environ.get('WEED_S3_DOMAIN'),
        aws_access_key_id=os.environ.get('WEED_S3_ACCESS_KEY'),
        aws_secret_access_key=os.environ.get('WEED_S3_SECRET_KEY')
    )

    image_key = f"ddm/{target}/{dsoc_uuid}.png"

    for attempt in range(5):
        try:
            if hasattr(image_file, 'read'):
                image_file.seek(0) # Reset stream pointer to the beginning of the file
                file_data = image_file.read()
            else:
                file_data = image_file
            s3.put_object(
                Bucket=os.environ.get('WEED_S3_BUCKET'),
                Key=image_key,
                Body=file_data,
                ContentType='image/png'
            )
            break
        except EndpointConnectionError:
            if attempt == 4:
                raise
            time.sleep(2)
            print(f"Success: Image saved to SeaweedFS at {image_key}")

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
            gbt_uuid = msg.key().decode("utf-8")

            #use the uuid from the payload to import the correct line of data from the GBT table:
            gbt_data = DB_import(gbt_uuid)

            #calculate latency with event_time from the GBT import, before we update the event_time value in DB_columns
            dsoc_latency = latency_calc(gbt_data[3])

            #create the rest of the column values specific to DSOC/images:
            data = DB_columns(gbt_data)
            data['latency_ms'] = dsoc_latency

            # 1. Gather data and create the image file and dsoc_uuid first:
            object_id, target, tx_waveform, event_time = gbt_data
            image_file, num_bytes = create_img(tx_waveform)
            dsoc_uuid = str(uuid.uuid4())

             # 2. Upload the image to SeaweedFS using your pre-generated uuid
            image_key = save_image_to_seaweedfs(target, image_file, dsoc_uuid)

            # 3. Inject the UUID and image_key directly into the payload data
            data['uuid'] = dsoc_uuid

            # 4. Save everything at once
            # To trigger the signal events to obsevent table
            publish_DB(image_key=image_key, num_bytes=num_bytes, data=data)

            print(f"Received message from {Stations.GBT.label} for object {object_id}; DDM is ready in SeaweedFS (Image Path: {image_key}.")
    
    except Exception as e:
        import traceback
        print("An unhandled exception occurred in the consumer loop:")
        traceback.print_exc()





#   except KeyboardInterrupt: 
#     pass
#   finally:
#     print("reached the end")
  


class Command(BaseCommand):
    help = "Runs the DSOC simulator"

    def handle(self, *args, **options):
        print("Starting DSOC simulator")

        consume(topic, config)