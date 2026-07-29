from django.core.management.base import BaseCommand

from ngRadar_Website.utils import bootstrap, consume
from ngRadar_Website.enums import Stations
from ngRadar_Website.models.models import gbtEvent


"""
This code will:
- consume a Kafka message from GBT inlcuding uuid, indicating that a new signal is transmitting
- use etc to send stored (or randomly generated) data to DSOC
- produce a Kafka message to DSOC that e-transfer started, including the uuid 
- continue listening for Kafka messages
"""


def process_msg(msg, producer_topic=None, producer_config=None):
    #decode the GBT payload that is a single string of just the uuid:
    gbt_uuid = msg.key().decode("utf-8")


class Command(BaseCommand):
    help = "Runs the VLBA simulator"

    def handle(self, *args, **options):
        print("Starting VLBA simulator")

        topic, config = bootstrap(Stations.DSOC)

        topic, config = bootstrap(Stations.DSOC)

        consume(topic, config, process_msg)