import subprocess

from django.core.management.base import BaseCommand
from confluent_kafka import Producer
from ngRadar_Website.utils import bootstrap, consume
from ngRadar_Website.enums import Stations
from ngRadar_Website.models.models import gbtEvent
from pathlib import Path


"""
This code will:
- consume a Kafka message from GBT inlcuding uuid, indicating that a new signal is transmitting
- use etc to send stored (or randomly generated) data to DSOC
- produce a Kafka message to DSOC that e-transfer started, including the uuid 
- continue listening for Kafka messages

Note: I am going to treat this sim as the Hancock VLBA site (Stations.HN) for hard-coded station data
    I chose Hancock because I am from New Hampshire and I wanted to

"""

COMMAND_DIR = Path("insert_path_to_etransfer/architecture/")  # TODO get path for etransfer
CLIENT_SOURCE = Path("/data/new/*")  # TODO figure out what to put here
DAEMON_DESTINATION = "localhost:/data/"  # TODO figure out what to put here


def produce(topic, config, key, value):
    # creates a new producer instance
    producer = Producer(config)

    # producing a message to the specified topic 
    producer.produce(topic, key=key, value=value)
    print(f"Produced message to topic {topic} with key {key}.")

    # send any outstanding or buffered messages to the Kafka broker
    producer.flush()


def process_msg(msg, producer_topic, producer_config):
    #decode the GBT payload that is a single string of just the uuid:
    gbt_uuid = msg.key().decode("utf-8")

    key, value = f"{gbt_uuid}", "e-transfer started"
    # produce this new message, lets DSOC know to produce image(s)
    produce(producer_topic, producer_config, key, value)


def start_etransfer():
    # TODO put this block in vlba docker compose
    # run etd command somehow
    # volumes:
    #   - ./etransfer_data/vlba:/data  # contains new and transferred folders?

    # TODO put this block in dsoc docker compose
    # run etd command somehow unless this only needs to be done once
    # volumes:
    #   - ./etransfer_data/dsoc:/data  # includes raw and processed folders?

    cmd = ["./etc", f"{CLIENT_SOURCE}", f"{DAEMON_DESTINATION}"]  # TODO figure out where to access etc

    result = subprocess.run(
        cmd,
        cwd=COMMAND_DIR,
        capture_output=True,  # set False if you want command output to print to the terminal
        text=True,
        check=False,  # set True if you want exceptions on non-zero exit
    )


class Command(BaseCommand):
    help = "Runs the VLBA simulator"

    def handle(self, *args, **options):
        print("Starting VLBA simulator")

        producer_topic, producer_config, consumer_topic, consumer_config = bootstrap(Stations.HN) # TODO: update according to Luara's bootstrap changes -THIS SHOULD BE GOOD NOW!

        consume(consumer_topic, consumer_config, process_msg, producer_topic=producer_topic, producer_config=producer_config)