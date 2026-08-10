from datetime import datetime, timezone
import uuid
from django.core.management.base import BaseCommand
from ngRadar_Website.models.models import ETransferEvent
from ngRadar_Website.enums import Stations, Status
from ngRadar_Website.utils import latency_calc, bootstrap, consume, write_transfer_progress
from pathlib import Path
import json
import uuid
import time


etd_path = Path("/dsoc/incoming")


def process_msg(msg, producer_topic=None, producer_config=None):
    payload = json.loads(msg.value().decode("utf-8"))
    gbt_uuid = uuid.UUID(payload["gbt_uuid"])
    #print(gbt_uuid)

    while True:
        while True:
            etransfer_data = (
            ETransferEvent.objects
            .filter(gbt_uuid=gbt_uuid, status=4)
            .values_list("transfer_uuid", "num_bytes")
            .first()
            )

            if etransfer_data is None:
                time.sleep(0.5)
            else:
                transfer_uuid, num_bytes = etransfer_data
                break

        incoming_file = etd_path / f"{transfer_uuid}.bin"
        #print(incoming_file)

        if num_bytes == 0:
            yield process_msg(event="progress_error",
                                data=json.dumps({"message": "num_bytes is 0"}))
            break

        while True:
            if not incoming_file.exists():
                time.sleep(0.5)
                continue

            received_bytes = incoming_file.stat().st_size
            percent = (received_bytes / num_bytes * 100)

            write_transfer_progress(
                received_bytes=received_bytes,
                total_bytes=num_bytes,
                percent=f"{percent:.1f}",
                transfer_id=transfer_uuid,
            )
            time.sleep(0.5)

            if received_bytes >= num_bytes:
                break

        break










class Command(BaseCommand):
    help = "Runs the etr progress writer"

    def handle(self, *args, **options):
        print("Poop")

        topic, config = bootstrap(Stations.ETR)

        consume(topic, config, process_msg)