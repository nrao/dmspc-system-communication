from datetime import datetime, timezone
import uuid
from django.core.management.base import BaseCommand
from ngRadar_Website.models.models import gbtEvent, dsocEvent, ETransferEvent
from ngRadar_Website.enums import Stations, Status
from ngRadar_Website.utils import latency_calc, bootstrap, consume, create_s3_client, upload_seaweedfs
from pathlib import Path
import json
import uuid
import time

load_dotenv()

etd_path = Path("/dsoc/incoming")



def file_size_bytes(p):
    return p.stat().st_size  # size in bytes

size_in_bytes = file_size_bytes(etd_path)
print(size_in_bytes)


def process_msg(msg, producer_topic=None, producer_config=None):







class Command(BaseCommand):
    help = "Runs the etr progress writer"

    def handle(self, *args, **options):
        print("Poop")

        topic, config = bootstrap(Stations.DSOC)

        consume(topic, config, process_msg)