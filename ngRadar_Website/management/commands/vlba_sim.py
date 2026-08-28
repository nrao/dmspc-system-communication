import json
import subprocess
import uuid

from django.core.management.base import BaseCommand
from confluent_kafka import Producer
from ngRadar_Website.utils import *
from ngRadar_Website.enums import Stations, Status, Message
from confluent_kafka import Producer
from pathlib import Path
from django.utils import timezone
from ngRadar_Website.models.models import gbtEvent, ETransferEvent
from datetime import datetime, timezone
from threading import Thread


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

FAILURE_REASONS = {
    -9: "The e-transfer process was terminated",
    -6: "The connection to the e-transfer daemon was lost",
}

# A daemon outage costs one attempt, since wait_for_etd() does the waiting. This
# cap is for failures the daemon being up cannot fix (missing source file, full
# disk), which would otherwise retry at full speed forever.
MAX_RESUME_ATTEMPTS = 5


def process_msg(msg, producer_topic, producer_config):
    incoming_key = int(msg.key().decode("utf-8"))
    raw_data_path = Path("/raw_data")
    
    if incoming_key == Message.GBT_TX.value:
        print("Received Kafka message from GBT.")
        key = f"{Message.VLBA_REQUEST_STORAGE}"
        gbt_uuid = msg.value().decode("utf-8")
        transfer_uuid = uuid.uuid4()

        frame_path = raw_data_path / f"{transfer_uuid}.bin"
    
        Thread(target=create_file, args=(frame_path,), daemon=True).start()
    
        watch_for_file(frame_path)
    
        if frame_path.is_file():
            num_bytes = frame_path.stat().st_size
    
            record_transfer_event(
                    transfer_uuid=transfer_uuid,
                    gbt_uuid=gbt_uuid,
                    station=Stations.HN,
                    status=Status.READY,
                    num_bytes=num_bytes,
                    message="Hancock VLBA data file complete. Ready for e-transfer.",
                )
            send_kafka_message(
                key = key,
                producer_topic=producer_topic,
                producer_config=producer_config,
                transfer_uuid=transfer_uuid,
                gbt_uuid=gbt_uuid,
                status=Status.READY,
                num_bytes=num_bytes,
                filename=frame_path.name,
                message=1,
            )
            print("VLBA requesting DSOC check storage...")

        else:
            send_kafka_message(
                key = key,
                producer_topic=producer_topic,
                producer_config=producer_config,
                transfer_uuid=transfer_uuid,
                gbt_uuid=gbt_uuid,
                status=Status.FAILED,
                num_bytes=0,
                filename=frame_path.name,
                message="Source file does not exist",
            )
            print("Source file does not exist.")
            # Nothing will ever make this file appear, so mark the message done
            # instead of leaving it uncommitted for a replay that must fail again.
            return True

    
    elif incoming_key == Message.DSOC_RESPOND_STORAGE.value:
        print("Received DSOC's storage check response!")
        key = f"{Message.VLBA_TRANSFERRING}"
        payload = json.loads(msg.value().decode("utf-8"))

        if payload["message"] == "Yes":

            attempts = 0

            # If etr_daemon dies, etc exits non-zero and we record FAILED, wait for
            # the daemon to answer again, then let this loop replay the whole block.
            while True:

                try:
                    record_transfer_event(
                        transfer_uuid=payload["transfer_uuid"],
                        gbt_uuid=payload["gbt_uuid"],
                        station=Stations.HN,
                        status=Status.TRANSFERRING,
                        num_bytes=payload["num_bytes"],
                        message="Hancock VLBA e-transfer in progress",
                    )

                    send_kafka_message(
                        key = key,
                        producer_topic=producer_topic,
                        producer_config=producer_config,
                        transfer_uuid=payload["transfer_uuid"],
                        gbt_uuid=payload["gbt_uuid"],
                        status=Status.TRANSFERRING,
                        num_bytes=payload["num_bytes"],
                        filename=payload["filename"],
                        message="Hancock VLBA has started to send the data file to DSOC via e-transfer",
                    )
                    frame_path = raw_data_path / f"{payload['transfer_uuid']}.bin"

                    print("DSOC responded affirmative to storage check. Initiating e-transfer...")
                    etc_send(frame_path)
                    break

                except subprocess.CalledProcessError as exc:
                    print(f"E-transfer failed with return code: {exc.returncode}")
                    record_transfer_event(
                        transfer_uuid=payload["transfer_uuid"],
                        gbt_uuid=payload["gbt_uuid"],
                        station=Stations.HN,
                        status=Status.FAILED,
                        num_bytes=payload["num_bytes"],
                        message=(
                            f"{FAILURE_REASONS.get(exc.returncode, 'The e-transfer failed')} "
                            f"mid-transfer. Transfer interrupted. "
                            f"(return code: {exc.returncode})"
                        ),
                    )
                    attempts += 1
                    if attempts >= MAX_RESUME_ATTEMPTS:
                        print(f"E-transfer failed {attempts} times. Giving up.")
                        return False

                    print("Waiting for the e-transfer daemon to come back...")
                    if not wait_for_etd():
                        print("E-transfer daemon never came back. Giving up.")
                        return False
                    print("E-transfer daemon is back. Resuming the transfer...")

                except Exception as exc:
                    print(f"Unexpected e-transfer failure: {exc}")
                    record_transfer_event(
                        transfer_uuid=payload["transfer_uuid"],
                        gbt_uuid=payload["gbt_uuid"],
                        station=Stations.HN,
                        status=Status.FAILED,
                        num_bytes=payload["num_bytes"],
                        message=f"The e-transfer failed unexpectedly mid-transfer. Transfer interrupted. ({exc})",
                    )
                    return False

        # If DSOC does NOT have storage, VLBA will sleep and ask again.
        else: 
            print(f"DSOC responded negative to storage check. Will ask again in 5 seconds...")
            time.sleep(5)
            send_kafka_message(
                key = f"{Message.VLBA_REQUEST_STORAGE}",
                producer_topic=producer_topic,
                producer_config=producer_config,
                transfer_uuid=payload["transfer_uuid"],
                gbt_uuid=payload["gbt_uuid"],
                status=Status.READY,
                num_bytes=payload["num_bytes"],
                filename=payload["filename"],
                message=payload["message"],
            )

    elif incoming_key == Message.VLBA_DELETE.value:
        payload = json.loads(msg.value().decode("utf-8"))
        file_name = payload["filename"]
        delete_observation_data(file_name)

    else:
        print("NOT A VALID KAFKA MESSAGE VALUE!")

    return True


class Command(BaseCommand):
    help = "Runs the VLBA simulator"

    def handle(self, *args, **options):
        print("Starting VLBA simulator")

        producer_topic, producer_config, consumer_topic, consumer_config = bootstrap(Stations.HN)

        # process_msg blocks while wait_for_etd() waits for etr_daemon to come back
        consumer_config["max.poll.interval.ms"] = (
            (ETD_MAX_CONN_RETRY * ETD_RETRY_CONN_DELAY) + 300
        ) * 1000

        consume(
            consumer_topic,
            consumer_config,
            process_msg,
            producer_topic=producer_topic,
            producer_config=producer_config,
            manual_commit=True,
        )
