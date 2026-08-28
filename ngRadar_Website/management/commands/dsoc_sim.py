from datetime import datetime, timezone
import uuid
from django.core.management.base import BaseCommand
from confluent_kafka import Producer
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import io
from ngRadar_Website.models.models import gbtEvent, dsocEvent, ETransferEvent
from ngRadar_Website.enums import Stations, Status, Message
# from ngRadar_Website.utils import latency_calc, bootstrap, consume, create_s3_client, upload_seaweedfs, write_transfer_progress, send_kafka_message, get_folder_size
from ngRadar_Website.utils import *
from pathlib import Path
import json
import uuid
import time
from itertools import groupby
import os
import subprocess


"""
This code will:
- consume a message from the VLBA that data is being e-transferred
- pull the record from the GBT table using uuid sent in Kafka message from VLBA
- generate an image file using random data (not e-transferred data yet)
- save image file to seaweedfs object store
- load the image key + the uuid into the DB
"""

# How long the file may sit at the same size before DSOC asks the broker whether
# vlba is alive. Only a trigger for that question, never a verdict: quiet bytes
# mean a slow transfer as often as a dead sender. Shorter than SESSION_TIMEOUT_MS
# only delays the FAILED row by a round, since the clock re-arms and asks again.
STALL_TIMEOUT_SECONDS = 15


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



def publish_dsocEvents(
    *,
    image_key,
    num_bytes,
    data,
    xmit_station,
    rcvr_station,
    transfer_uuid,
):
    payload_data = data.copy()


    payload_data.update({
        "image_key": image_key,
        "num_bytes": num_bytes,
        "xmit_station": xmit_station,
        "rcvr_station": rcvr_station,
        "transfer_uuid": transfer_uuid,
        "status": Status.COMPLETED,
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

    try:
        image_key = f"ddm/{target}/{dsoc_uuid}.png"

        s3 = create_s3_client()
        
        file_data = image_file

        image_key = upload_seaweedfs(s3, image_key, file_data)

        print(f"Success: Image saved to SeaweedFS at {image_key}")

        return image_key
    except:
        publish_status_obsEvents(
            status=Status.FAILED,
            msg="Failed to connect to SeaweedFS.",
        )
        return False


# Verifies that the incoming file exists and has the expected number of bytes that VLBA sent in the kafka message.
def verify_incoming_transfer(
    *,
    incoming_file,
    expected_num_bytes,
    attempts=10,
    delay_seconds=0.5,
    ):
        for _ in range(attempts):
            if incoming_file.is_file():
                actual_num_bytes = incoming_file.stat().st_size

                if actual_num_bytes == expected_num_bytes:
                    return actual_num_bytes

            time.sleep(delay_seconds)

        raise RuntimeError(
            f"Transfer verification failed for {incoming_file}. "
            f"Expected {expected_num_bytes} bytes."
        )


def track_etransfer_progress(payload, incoming_file: Path):
    transfer_uuid = payload["transfer_uuid"] # syntax?
    num_bytes = payload["num_bytes"] # make this an int?
    received_bytes = 0
    write_transfer_progress( # resetting progress.json to zero so below logic doesn't read from previous run. Submit button does this too, but not on system-up :(
        received_bytes=0,
        total_bytes=0,
        percent=0,
        transfer_id=0,
    )
    print("Transfer in progress...")
    last_progress_at = time.monotonic()
    while ETransferEvent.objects.filter(transfer_uuid=transfer_uuid).order_by("-event_time").values_list("status", flat=True).first() == Status.TRANSFERRING:
        # while loop will break prematurely if status in DB ever changes - ex. if it changes to FAILED mid e-transfer.
        if incoming_file.exists():
            current_bytes = incoming_file.stat().st_size
            if current_bytes > received_bytes:
                last_progress_at = time.monotonic()
            received_bytes = current_bytes
            percent = (received_bytes / num_bytes * 100)

            write_transfer_progress(
                received_bytes=received_bytes,
                total_bytes=num_bytes,
                percent=f"{percent:.1f}",
                transfer_id=f"{transfer_uuid}",
            )
            time.sleep(0.5)
        else:
            time.sleep(0.5)

        if received_bytes >= num_bytes:
            print(f"Transfer of <{transfer_uuid}.bin> COMPLETE.")
            break

        if time.monotonic() - last_progress_at > STALL_TIMEOUT_SECONDS:
            if consumer_group_has_members(f"{Stations.HN.name.lower()}-consumer-group"):
                # vlba is alive, the transfer is just slow. Start the clock over.
                last_progress_at = time.monotonic()
            else:
                record_transfer_event(
                    transfer_uuid=transfer_uuid,
                    gbt_uuid=payload["gbt_uuid"],
                    station=Stations.DSOC,
                    status=Status.FAILED,
                    num_bytes=num_bytes,
                    message="Hancock VLBA went offline mid-transfer. Transfer interrupted.",
                )
                break

    if ETransferEvent.objects.filter(transfer_uuid=transfer_uuid).order_by("-event_time").values_list("status", flat=True).first() == Status.FAILED:
        raise ValueError("E-Transfer client reported a FAILED status mid-transfer.")

    if received_bytes != num_bytes:
        raise ValueError("Transfer progress has halted. Not all bytes have been received!!")

 
def process_msg(msg, producer_topic, producer_config):
    incoming_key = int(msg.key().decode("utf-8"))
    payload = json.loads(msg.value().decode("utf-8"))
    volume_folder = Path("/dsoc/incoming")
    
    if incoming_key == Message.VLBA_REQUEST_STORAGE.value:
        # storage check logic
        expected_num_bytes = int(payload["num_bytes"])

        key = f"{Message.DSOC_RESPOND_STORAGE}" #produced message will have this key no matter what the result of the below logic is
        if payload["status"] == Status.FAILED:
            record_transfer_event(
                transfer_uuid=payload["transfer_uuid"],
                gbt_uuid=payload["gbt_uuid"],
                station=Stations.HN,
                status=Status.FAILED,
                num_bytes=payload["num_bytes"],
                message=payload["message"],
            )
            print("The raw data file does not exist.")

        else:
            storage_limit = int(os.environ["DSOC_VOLUME_SIZE"]) * 1000000000
            print(f"DSOC has {storage_limit/1000000000:0.2f}GB of storage total.")
            storage_used = int(get_folder_size(volume_folder))
            space_remaining = (storage_limit/1000000000)-(storage_used/1000000000)
            print(f"DSOC has {space_remaining:0.2f}GB of storage remaining.")
            if storage_used+expected_num_bytes >= storage_limit:
                # if the current storage plus the incoming file exceeds our imposed limit, we decline the e-transfer
                if payload["message"] == 15:
                    record_transfer_event(
                        transfer_uuid=payload["transfer_uuid"],
                        gbt_uuid=payload["gbt_uuid"],
                        station=Stations.HN,
                        status=Status.FAILED,
                        num_bytes=payload["num_bytes"],
                        message=f"DSOC does not have enough storage. Failed 15 times.",
                    )
                    print("DSOC failed to clear storage in 15 tries. Try again manually later.")

                else: 
                    if payload["message"] == 1:
                        # The FAILED record only gets saved to the DB the first time. The payload message for any storage check retries will contain message=2 
                        record_transfer_event(
                            transfer_uuid=payload["transfer_uuid"],
                            gbt_uuid=payload["gbt_uuid"],
                            station=Stations.HN,
                            status=Status.RETRYING,
                            num_bytes=payload["num_bytes"],
                            message=f"DSOC does not have enough storage. Retrying...",
                        )
                    send_kafka_message(
                        key = key, 
                        producer_topic=producer_topic,
                        producer_config=producer_config, 
                        transfer_uuid=payload["transfer_uuid"],
                        gbt_uuid=payload["gbt_uuid"],
                        status=payload["status"],
                        num_bytes=payload["num_bytes"],
                        filename=payload["filename"],
                        message=payload["message"]+1,
                    )
                    print(f"DSOC does not have enough storage to accept the data transfer request. The remaining disk space is {space_remaining:0.2f}GB and the incoming data is {expected_num_bytes/1000000000:0.2f}GB")
                    

            else:
                if payload["message"] != 1:
                    record_transfer_event(
                        transfer_uuid=payload["transfer_uuid"],
                        gbt_uuid=payload["gbt_uuid"],
                        station=Stations.HN,
                        status=Status.READY,
                        num_bytes=payload["num_bytes"],
                        message=f"DSOC made room to to accept the incoming data from {Stations.HN.label}",
                    )     
                    
                send_kafka_message(
                    key = key, 
                    producer_topic=producer_topic,
                    producer_config=producer_config, 
                    transfer_uuid=payload["transfer_uuid"],
                    gbt_uuid=payload["gbt_uuid"],
                    status=payload["status"],
                    num_bytes=payload["num_bytes"],
                    filename=payload["filename"],
                    message="Yes",
                )
                print("DSOC has enough storage to accept the incoming data. Awaiting e-transfer...")


    elif incoming_key == Message.VLBA_TRANSFERRING.value:
        payload = json.loads(msg.value().decode("utf-8")) 
        key = f"{Message.VLBA_DELETE}"
        incoming_file = volume_folder / f"{payload['transfer_uuid']}.bin"

        try:
            track_etransfer_progress(payload, incoming_file)

            record_transfer_event(
                transfer_uuid=payload["transfer_uuid"],
                gbt_uuid=payload["gbt_uuid"],
                station=Stations.HN,
                status=Status.TRANSFERRED,
                num_bytes=payload["num_bytes"],
                message="Hancock VLBA e-transfer complete",
            )
            record_transfer_event(
                transfer_uuid=payload["transfer_uuid"],
                gbt_uuid=payload["gbt_uuid"],
                station=Stations.DSOC,
                status=Status.VERIFYING,
                num_bytes=payload["num_bytes"],
                message=f"Verifying {payload['filename']}",
            )

        except Exception as exc:
            print(f"Incoming data progress interrupted: {exc}")
            return


        try:
            actual_num_bytes = verify_incoming_transfer( 
                incoming_file=incoming_file,
                expected_num_bytes=payload["num_bytes"],
            )
        except Exception as exc:
            record_transfer_event(
                transfer_uuid=payload["transfer_uuid"],
                gbt_uuid=payload["gbt_uuid"],
                station=Stations.DSOC,
                status=Status.FAILED,
                num_bytes=0,
                message=str(exc),
            )
            return

        try:
            gbt_data = DB_import(payload["gbt_uuid"])
            dsoc_latency = latency_calc(gbt_data[3])

            data = DB_columns(gbt_data)
            data["latency_ms"] = dsoc_latency

            object_id, target, tx_waveform, event_time = gbt_data
            image_file, image_num_bytes = create_img(tx_waveform)
            dsoc_uuid = str(uuid.uuid4())

            image_key = save_image_to_seaweedfs(
                target,
                image_file,
                dsoc_uuid,
            )
            if image_key == False:
                return
            else:
                data["uuid"] = dsoc_uuid

                publish_dsocEvents(
                    image_key=image_key,
                    num_bytes=image_num_bytes,
                    data=data,
                    xmit_station=Stations.GBT,
                    rcvr_station=Stations.HN,
                    transfer_uuid=payload["transfer_uuid"],
                )

        except Exception as exc:
            record_transfer_event(
                transfer_uuid=payload["transfer_uuid"],
                gbt_uuid=payload["gbt_uuid"],
                station=Stations.DSOC,
                status=Status.FAILED,
                num_bytes=payload["num_bytes"],
                message=f"DSOC image processing failed: {exc}",
            )
            return

        record_transfer_event(
            transfer_uuid=payload["transfer_uuid"],
            gbt_uuid=payload["gbt_uuid"],
            station=Stations.DSOC,
            status=Status.COMPLETED,
            num_bytes=actual_num_bytes,
            latency_ms=dsoc_latency,
            message="DSOC has verified etransfer, image generated, and image stored.",
        )

        send_kafka_message(
            key = key, 
            producer_topic=producer_topic,
            producer_config=producer_config, 
            transfer_uuid=payload["transfer_uuid"],
            gbt_uuid=payload["gbt_uuid"],
            status=payload["status"],
            num_bytes=payload["num_bytes"],
            filename=payload["filename"],
            message="Processing complete. Delete your raw data.",
        )
        
    else:
        print("Invalid Kafka Message Key!")


class Command(BaseCommand):
    help = "Runs the DSOC simulator"

    def handle(self, *args, **options):
        print("Starting DSOC simulator")

        producer_topic, producer_config, consumer_topic, consumer_config = bootstrap(Stations.DSOC)

        consume(consumer_topic, consumer_config, process_msg, producer_topic=producer_topic, producer_config=producer_config)