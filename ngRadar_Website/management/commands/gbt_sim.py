from datetime import datetime
import os
from django.core.management.base import BaseCommand
from confluent_kafka import Producer
from confluent_kafka import Consumer
from ngRadar_Website.enums import Stations
from ngRadar_Website.models.models import uiEvent
from ngRadar_Website.models.models import gbtEvent
# from dotenv import find_dotenv
from pathlib import Path

# path = find_dotenv()  # searches upward for a .env
# print("find_dotenv():", path)

from confluent_kafka.admin import AdminClient, NewTopic


p = Path("../../../../out/ngrok_endpoint.env")
text = p.read_text().strip()

bootstrap = None
for line in text.splitlines():
    if line.startswith("BOOTSTRAP_SERVER="):
        bootstrap = line.split("=", 1)[1].strip()
        break

if not bootstrap:
    raise RuntimeError("BOOTSTRAP_SERVER not found in /out/ngrok_endpoint.env")

#bootstrap = os.environ["BOOTSTRAP_SERVER"] 
admin = AdminClient({"bootstrap.servers": bootstrap})
topics = [
    NewTopic("user_input", num_partitions=3, replication_factor=1),
    NewTopic("GBT_data", num_partitions=1, replication_factor=1),
]
fs = admin.create_topics(topics, request_timeout=30)

for topic, f in fs.items():
    # f is a Future; result() will raise if creation failed for reasons other than "already exists"
    f.result()


# payload that will be inserted in the gbtEvent db table
payload = {
    "object_id": None, 
    "target": None, 
    "tx_waveform": None, 
    "rec_waveform": None, 
    "event_time": None, 
    "latency_ms": None,
}

producer_topic = "GBT_data"  # NOTE The topic to which the messages will be sent, rename accordingly to whatever topic you want to send the DDM payloads to.
producer_config = {
    "bootstrap.servers": bootstrap,
    "message.max.bytes": 8388608,
    "client.id": "GBT-producer"
}

consumer_topic = "user_input"  # NOTE Might want to change name
consumer_config = {
    "bootstrap.servers": bootstrap,
    "fetch.max.bytes": 8388608,
    "session.timeout.ms": 45000,
    "client.id": "GBT-consumer",
    "group.id": "GBT-consumer-group",
    "auto.offset.reset": "earliest",
}


def set_payload_dict(waveform):
    payload["object_id"] = '30104'
    payload["target"] = 'Moretus'
    payload["tx_waveform"] = waveform
    payload["rec_waveform"] = waveform
    payload["event_time"] = datetime.now()
    payload["latency_ms"] = latency_calc(payload["event_time"])


def latency_calc(event_time):
    # calculates the latency of the message from the time it was sent to the time it was received
    # returns latency in milliseconds
    current_time = datetime.now()
    latency = current_time - event_time
    latency_ms = latency.total_seconds() * 1000
    return latency_ms


def generate_payload(ui_event_uuid):
    ui_event = uiEvent.objects.get(uuid=ui_event_uuid)

    set_payload_dict(ui_event.selected_waveform)


def publish_to_db():
    gbt_event = gbtEvent.objects.create(**payload)

    return gbt_event.uuid


def produce(topic, config, key, value):
    # creates a new producer instance
    producer = Producer(config)

    # producing a message to the specified topic 
    producer.produce(topic, key=key, value=value)
    print(f"Produced message to topic {topic} with key {key}.")

    # send any outstanding or buffered messages to the Kafka broker
    producer.flush()


def consume(topic, config):
    # creates a new consumer instance
    consumer = Consumer(config)

    #subscribes to the specified topic
    consumer.subscribe([topic])

    try:
        while True:
            # consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0) # polls for messages for 1 second
            # if msg is not None and msg.error() is None:
            if msg is None:
                continue
            if msg.error() is not None:
                print("Consumer error:", msg.error())
                continue

            ui_uuid = msg.key().decode("utf-8")  # this is the uuid of the ui_event
            notif = msg.value().decode("utf-8")

            # fill in the values to be published to the db
            generate_payload(ui_uuid)

            # publish new transmission to the db
            gbt_uuid = publish_to_db()

            key, value = f"{gbt_uuid}", "GBT transmitting"

            # produce this new message, lets DSOC know to produce image(s)
            produce(producer_topic, producer_config, key, value)

    except KeyboardInterrupt: 
        pass
    finally:
        # closes the consumer connection
        consumer.close()


class Command(BaseCommand):
    help = "Runs the GBT simulator"

    def handle(self, *args, **options):
        print("Starting GBT simulator")

        # generate a dummy data payload, publish this data to the db, produce a message with this payload, then start consuming
        set_payload_dict('W48')
        gbt_uuid = publish_to_db()
        key, value = f"{gbt_uuid}", "GBT transmitting"
        produce(producer_topic, producer_config, key, value)
        consume(consumer_topic, consumer_config)