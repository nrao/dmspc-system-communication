from datetime import datetime, timezone
import uuid
from django.core.management.base import BaseCommand
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import io
from ngRadar_Website.models.models import gbtEvent, dsocEvent, ETransferEvent
from ngRadar_Website.enums import Stations, Status
from ngRadar_Website.utils import latency_calc, bootstrap, consume, create_s3_client, upload_seaweedfs
from pathlib import Path
import json
import uuid
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




def publish_DB(
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

    image_key = f"ddm/{target}/{dsoc_uuid}.png"

    s3 = create_s3_client()
    
    file_data = image_file

    image_key = upload_seaweedfs(s3, image_key, file_data)

    print(f"Success: Image saved to SeaweedFS at {image_key}")

    return image_key



# Verifies that the incoming file exists and has the expected number of bytes that VLBA sent in the kafka message.
# Don't love this. We should
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


# Helper function to record the status of the e-transfer in the ETransferEvent table
def record_transfer_event(
    *,
    transfer_uuid,
    gbt_uuid,
    station,
    status,
    num_bytes=0,
    latency_ms=0.0,
    message="",
):
    gbt_event = gbtEvent.objects.get(uuid=gbt_uuid)

    return ETransferEvent.objects.create(
        transfer_uuid=transfer_uuid,
        gbt_uuid=gbt_uuid,
        object_id=gbt_event.object_id,
        target=gbt_event.target,
        station=station,
        event_time=datetime.now(timezone.utc),
        latency_ms=latency_ms,
        num_bytes=num_bytes,
        status=status,
        message=message,
    )
    


def process_msg(msg, producer_topic=None, producer_config=None):
    payload = json.loads(msg.value().decode("utf-8"))

    transfer_uuid = uuid.UUID(payload["transfer_uuid"])
    gbt_uuid = uuid.UUID(payload["gbt_uuid"])
    status = Status(payload["status"])
    filename = payload.get("filename")
    expected_num_bytes = payload.get("num_bytes", 0)
    message = payload.get("message", "")
    incoming_file = Path("/dsoc/incoming") / filename

    record_transfer_event(
        transfer_uuid=transfer_uuid,
        gbt_uuid=gbt_uuid,
        station=Stations.HN,
        status=status,
        num_bytes=expected_num_bytes,
        message=message,
    )

    # filename = payload.get("filename")
    # incoming_file = Path("/dsoc/incoming") / filename
    while incoming_file.stat().st_size <= expected_num_bytes:
        # print received bytes out of expected bytes and then write to a json file to be read by the progress bar on the front end
        print(f"Received {incoming_file.stat().st_size} out of {expected_num_bytes} bytes")
        print(f"Received {incoming_file.stat().st_size} out of {expected_num_bytes} bytes")
        progress_data = {
            "received_bytes": incoming_file.stat().st_size,
            "total_bytes": expected_num_bytes,
            "total_bytes": expected_num_bytes,
        }
        with open("/service/mock_assets/progress.json", "w") as f:
            json.dump(progress_data, f)
        time.sleep(0.5)  # Sleep for a short duration before checking again
        if incoming_file.stat().st_size == expected_num_bytes:
            break

    if status == Status.FAILED:
        return

    if status != Status.TRANSFERRED:
        return

    already_completed = ETransferEvent.objects.filter(
        transfer_uuid=transfer_uuid,
        station=Stations.DSOC,
        status=Status.COMPLETED,
    ).exists()

    if already_completed:
        print(f"This transfer {transfer_uuid} has already been processed. Skipping.")
        return

    already_processing = ETransferEvent.objects.filter(
        transfer_uuid=transfer_uuid,
        station=Stations.DSOC,
        status__in=[Status.VERIFYING],
        ).exists()

    if already_processing:
        print(f"Transfer {transfer_uuid} is already being processed currently.")
        return

    if not filename:
        record_transfer_event(
            transfer_uuid=transfer_uuid,
            station=Stations.DSOC,
            status=Status.FAILED,
            num_bytes=expected_num_bytes,
            message="Kafka transfer message did not contain a filename",
        )
        return


    record_transfer_event(
        transfer_uuid=transfer_uuid,
        gbt_uuid=gbt_uuid,
        station=Stations.DSOC,
        status=Status.VERIFYING,
        num_bytes=expected_num_bytes,
        message=f"Verifying {filename}",
    )

    try:
        actual_num_bytes = verify_incoming_transfer(
            incoming_file=incoming_file,
            expected_num_bytes=expected_num_bytes,
        )
    except Exception as exc:
        record_transfer_event(
            transfer_uuid=transfer_uuid,
            gbt_uuid=gbt_uuid,
            station=Stations.DSOC,
            status=Status.FAILED,
            num_bytes=0,
            message=str(exc),
        )
        return

    try:
        gbt_data = DB_import(gbt_uuid)
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

        data["uuid"] = dsoc_uuid

        publish_DB(
            image_key=image_key,
            num_bytes=image_num_bytes,
            data=data,
            xmit_station=Stations.GBT,
            rcvr_station=Stations.HN,
            transfer_uuid=transfer_uuid,
        )

    except Exception as exc:
        record_transfer_event(
            transfer_uuid=transfer_uuid,
            gbt_uuid=gbt_uuid,
            station=Stations.DSOC,
            status=Status.FAILED,
            num_bytes=expected_num_bytes,
            message=f"DSOC processing failed: {exc}",
        )
        return

    record_transfer_event(
        transfer_uuid=transfer_uuid,
        gbt_uuid=gbt_uuid,
        station=Stations.DSOC,
        status=Status.COMPLETED,
        num_bytes=actual_num_bytes,
        latency_ms=dsoc_latency,
        message="Transfer verified, transfer complete, image generated, and image stored.",
    )


class Command(BaseCommand):
    help = "Runs the DSOC simulator"

    def handle(self, *args, **options):
        print("Starting DSOC simulator")

        topic, config = bootstrap(Stations.DSOC)

        consume(topic, config, process_msg)