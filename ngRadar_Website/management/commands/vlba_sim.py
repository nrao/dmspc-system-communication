from django.core.management.base import BaseCommand

from ngRadar_Website.utils import bootstrap, consume
from ngRadar_Website.enums import Stations
from ngRadar_Website.models.models import gbtEvent


"""
This code will:
- consume a Kafka message from GBT, indicating that a new signal is transmitting including uuid
- pull new waveform from GBT table 
- produce a Kafka message to DSOC, including the uuid and new waveform being transmitted 
- use etc to send pre-set data to DSOC
- continue listening for Kafka messages
"""

def DB_import(uuid):
    
  gbt_data = gbtEvent.objects.filter(uuid=uuid).values_list('object_id', 'target', 'tx_waveform', 'event_time').first()

  return gbt_data


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