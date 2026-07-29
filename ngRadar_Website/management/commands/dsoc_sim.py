from datetime import datetime, timezone
import uuid
from django.core.management.base import BaseCommand
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import io
from ngRadar_Website.models.models import gbtEvent, dsocEvent
from ngRadar_Website.enums import Stations
from ngRadar_Website.utils import latency_calc, bootstrap, consume, create_s3_client, upload_seaweedfs
from botocore.exceptions import EndpointConnectionError, ClientError
import os
import time


"""
This code will:
- consume a message from the VLBA that data is being e-transferred
- pull the record from the GBT table using uuid sent in Kafka message from VLBA
- generate an image file using random data (not e-transferred data yet)
- save image file to seaweedfs object store
- load the image key + the uuid into the DB
"""

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
    # Saves the image to SeaweedFS using S3 API

    image_key = f"ddm/{target}/{dsoc_uuid}.png"

    s3 = create_s3_client()
    
    file_data = image_file

    image_key = upload_seaweedfs(s3, image_key, file_data)

    print(f"Success: Image saved to SeaweedFS at {image_key}")

    return image_key
        

def process_msg(msg, producer_topic=None, producer_config=None):
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


class Command(BaseCommand):
    help = "Runs the DSOC simulator"

    def handle(self, *args, **options):
        print("Starting DSOC simulator")

        topic, config = bootstrap(Stations.DSOC)

        consume(topic, config, process_msg)