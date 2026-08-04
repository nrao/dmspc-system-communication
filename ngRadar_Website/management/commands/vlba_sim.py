from django.core.management.base import BaseCommand
from confluent_kafka import Producer
from ngRadar_Website.utils import bootstrap, consume, etc_send
from ngRadar_Website.enums import Stations
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

    # after sending a kafka message, begin e-transfer NOTE we need the etransfer repo in our repo to get this working
    frame_path = Path("/service/mock_assets/large_data/BT161A1_PT_No0008.large")
    # ^ this is the location of the data vlba client is going to grab and send to dsoc daemon

    #NOTE: this is where we can add more logic to print status updates on data being generated at VLBA, and send once it's ready. For now, we are just sending a static file to DSOC daemon to test the e-transfer functionality.
    if frame_path.exists():
        print(f"Sending {frame_path} to DSOC daemon...")
        etc_send(frame_path)
    else:
        print(f"Error: {frame_path} does not exist. Cannot send to DSOC daemon.")




    


class Command(BaseCommand):
    help = "Runs the VLBA simulator"

    def handle(self, *args, **options):
        print("Starting VLBA simulator")

        producer_topic, producer_config, consumer_topic, consumer_config = bootstrap(Stations.HN) # TODO: update according to Luara's bootstrap changes -THIS SHOULD BE GOOD NOW!

        consume(consumer_topic, consumer_config, process_msg, producer_topic=producer_topic, producer_config=producer_config)