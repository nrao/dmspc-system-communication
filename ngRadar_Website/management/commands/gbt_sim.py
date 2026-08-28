from datetime import datetime, timezone
import time
from django.core.management.base import BaseCommand
from ngRadar_Website.enums import Stations, Message
from ngRadar_Website.models.models import uiEvent
from ngRadar_Website.models.models import gbtEvent
from ngRadar_Website.utils import latency_calc, bootstrap, consume, produce


# payload that will be inserted in the gbtEvent db table
payload = {
    "object_id": None, 
    "target": None, 
    "tx_waveform": None, 
    "rec_waveform": None, 
    "event_time": None, 
    "latency_ms": None,
}

def set_payload_dict(waveform, event_time):
    payload["object_id"] = '30104'
    payload["target"] = 'Moretus'
    payload["tx_waveform"] = waveform
    payload["rec_waveform"] = waveform
    payload["event_time"] = datetime.now(timezone.utc)
    payload["latency_ms"] = latency_calc(event_time, Stations.GBT)

    return payload


def generate_payload(ui_event_uuid):
    ui_event = uiEvent.objects.get(uuid=ui_event_uuid)

    payload = set_payload_dict(ui_event.selected_waveform, ui_event.event_time)

    return payload


def turn_off_transmitter():
    gbtEvent.objects.create(
        **
        {
            "object_id": '30104', 
            "target": 'Moretus', 
            "tx_waveform": 'Tx_OFF', 
            "rec_waveform": 'Tx_OFF', 
            "event_time": datetime.now(timezone.utc), 
            "latency_ms": 0,
        }
    )
    time.sleep(5)


def publish_gbtEvents(payload):
    gbt_event = gbtEvent.objects.create(**payload)

    return gbt_event.uuid


def process_msg(msg, producer_topic, producer_config):
    ui_uuid = msg.value().decode("utf-8")  # this is the uuid of the ui_event

    # turn off the transmitter for 5 seconds
    turn_off_transmitter()

    # fill in the values to be published to the db
    payload = generate_payload(ui_uuid)

    # publish new transmission to the db
    gbt_uuid = publish_gbtEvents(payload)

    key, value = f"{Message.GBT_TX}", f"{gbt_uuid}"

    # produce this new message, lets DSOC know to produce image(s)
    produce(producer_topic, producer_config, key, value)


class Command(BaseCommand):
    help = "Runs the GBT simulator"

    def handle(self, *args, **options):
        print("Starting GBT simulator")
        #time.sleep(10)
        producer_topic, producer_config, consumer_topic, consumer_config = bootstrap(Stations.GBT)

        # generate a dummy data payload, publish this data to the db, produce a message with this payload, then start consuming
        payload = set_payload_dict('W48', -1)
        gbt_uuid = publish_gbtEvents(payload)
        key, value = f"{Message.GBT_TX}", f"{gbt_uuid}"
        produce(producer_topic, producer_config, key, value)
        consume(consumer_topic, consumer_config, process_msg, producer_topic=producer_topic, producer_config=producer_config)