from django.core.management.base import BaseCommand
from confluent_kafka import Producer
from ngRadar_Website.utils import bootstrap, consume, etc_send
from ngRadar_Website.enums import Stations, Status
from pathlib import Path
import json
import subprocess
import uuid
from django.utils import timezone
from ngRadar_Website.models.models import gbtEvent, ETransferEvent
from datetime import datetime, timezone



"""
This code will:
- consume a Kafka message from GBT inlcuding uuid, indicating that a new signal is transmitting
- use etc to send stored (or randomly generated) data to DSOC
- produce a Kafka message to DSOC that e-transfer started, including the uuid 
- continue listening for Kafka messages

Note: I am going to treat this sim as the Hancock VLBA site (Stations.HN) for hard-coded station data
    I chose Hancock because I am from New Hampshire and I wanted to. 
    Stations enum: HN  = 91, "Hancock (25-m, VLBA)"

"""


def produce(topic, config, key, value):
    # creates a new producer instance
    producer = Producer(config)

    # producing a message to the specified topic 
    producer.produce(topic, key=key, value=value)
    print(f"Produced message to topic {topic} with key {key}.")

    # send any outstanding or buffered messages to the Kafka broker
    producer.flush()


def send_kafka_message(
    *,
    producer_topic,
    producer_config,
    transfer_uuid,
    gbt_uuid,
    status,
    num_bytes,
    filename=None,
    message="",
    stations=Stations.HN,
):
    payload = {
        "transfer_uuid": str(transfer_uuid),
        "gbt_uuid": str(gbt_uuid),
        "status": int(status),
        "status_label": status.label,
        "num_bytes": num_bytes,
        "filename": filename,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "message": message,
        "stations": stations.label,
    }

    produce(
        producer_topic,
        producer_config,
        str(transfer_uuid),
        json.dumps(payload),
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



def process_msg(msg, producer_topic, producer_config):
    gbt_uuid = msg.key().decode("utf-8")
    transfer_uuid = uuid.uuid4()
    # frame_path = Path("/service/mock_assets/large_data/BT161A1_PT_No0008.large")

    frame_path = Path("/service/testdata/hello.txt")

    record_transfer_event(
            transfer_uuid=transfer_uuid,
            gbt_uuid=gbt_uuid,
            station=Stations.HN,
            status=Status.READY,
            num_bytes=0,
            message="VLBA data file complete. Ready for e-transfer.",
        )


    if not frame_path.is_file():
        send_kafka_message(
            producer_topic=producer_topic,
            producer_config=producer_config,
            transfer_uuid=transfer_uuid,
            gbt_uuid=gbt_uuid,
            status=Status.FAILED,
            num_bytes=0,
            filename=frame_path.name,
            message="Source file does not exist",
        )
        return

    num_bytes = frame_path.stat().st_size

    try:
        record_transfer_event(
                transfer_uuid=transfer_uuid,
                gbt_uuid=gbt_uuid,
                station=Stations.HN,
                status=Status.TRANSFERRING,
                num_bytes=num_bytes,
                message="VLBA e-transfer in progress",
            )
        etc_send(frame_path)
    except subprocess.CalledProcessError as exc:
        send_kafka_message(
            producer_topic=producer_topic,
            producer_config=producer_config,
            transfer_uuid=transfer_uuid,
            gbt_uuid=gbt_uuid,
            status=Status.FAILED,
            num_bytes=num_bytes,
            filename=frame_path.name,
            message=(
                "E-transfer failed with return code: "
                f"{exc.returncode}"
            ),
        )
        return
    except Exception as exc:
        send_kafka_message(
            producer_topic=producer_topic,
            producer_config=producer_config,
            transfer_uuid=transfer_uuid,
            gbt_uuid=gbt_uuid,
            status=Status.FAILED,
            num_bytes=num_bytes,
            filename=frame_path.name,
            message=f"Unexpected e-transfer failure: {exc}",
        )
        return


    send_kafka_message(
        producer_topic=producer_topic,
        producer_config=producer_config,
        transfer_uuid=transfer_uuid,
        gbt_uuid=gbt_uuid,
        status=Status.TRANSFERRED,
        num_bytes=num_bytes,
        filename=frame_path.name,
        message="VLBA e-transfer completed successfully",
    )



class Command(BaseCommand):
    help = "Runs the VLBA simulator"

    def handle(self, *args, **options):
        print("Starting VLBA simulator")

        producer_topic, producer_config, consumer_topic, consumer_config = bootstrap(Stations.HN)

        consume(consumer_topic, consumer_config, process_msg, producer_topic=producer_topic, producer_config=producer_config)