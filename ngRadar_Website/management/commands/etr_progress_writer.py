from datetime import datetime, timezone
import uuid
from django.core.management.base import BaseCommand
from ngRadar_Website.models.models import gbtEvent, dsocEvent, ETransferEvent
from ngRadar_Website.enums import Stations, Status
from ngRadar_Website.utils import latency_calc, bootstrap, consume, write_transfer_progress
from pathlib import Path
import json
import uuid
import time


etd_path = Path("/dsoc/incoming")
# add the actual filename that comes from the ETransferEvent table


def file_size_bytes(p):
    return p.stat().st_size  # size in bytes

size_in_bytes = file_size_bytes(etd_path)
print(size_in_bytes)


def process_msg(msg, producer_topic=None, producer_config=None):
    payload = json.loads(msg.value().decode("utf-8"))

    gbt_uuid = uuid.UUID(payload["gbt_uuid"])

    while True:
        if not ETransferEvent.objects.filter(gbt_uuid=gbt_uuid, status__in=[Status.READY],).exists():
            time.sleep(0.5)
            continue

        try:
            etransfer_data = ETransferEvent.objects.filter(gbt_uuid=gbt_uuid, status__in=[Status.READY],).values_list('transfer_uuid', 'num_bytes').first()

            transfer_uuid = etransfer_data[0]
            total_bytes = etransfer_data[1]

            incoming_file = etd_path / f"{transfer_uuid}.bin"

            while True:

                received_bytes = incoming_file.stat().st_size
                percent = f"{(received_bytes / total_bytes * 100):.1f}"

                write_transfer_progress(received_bytes=received_bytes, total_bytes=total_bytes, percent=percent, transfer_id=transfer_uuid)
                time.sleep(0.5)

                if received_bytes == total_bytes:
                    break


        except Exception as e:
            yield process_msg(event="progress_error", data=json.dumps({"message": str(e)}))

        #time.sleep(0.2)
        break

        






class Command(BaseCommand):
    help = "Runs the etr progress writer"

    def handle(self, *args, **options):
        print("Poop")

        topic, config = bootstrap(Stations.DSOC)

        consume(topic, config, process_msg)