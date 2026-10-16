import json
import time
import uuid
from datetime import datetime, timezone
from django.core.management.base import BaseCommand

from ngRadar_Website.enums import (
    Stations,
    Status,
    Message,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter # or HTTP
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry import trace, propagate
from opentelemetry.trace import SpanKind, StatusCode
from opentelemetry.trace.status import Status as TraceStatus
from opentelemetry.context import attach, detach
from opentelemetry.sdk.resources import Resource

from ngRadar_Website.utils import (
    bootstrap,
    consume,
    latency_calc,
    send_kafka_message,
)
"""
GBT Simulator

This simulator:

- Consumes workflow events from Kafka.
- Turns the transmitter OFF and then ON with the requested waveform.
- Sends VLBA state/workflow events via Kafka.

This simulator does NOT write directly to the database (the ObservatoryEvent table).

The db_consumer is solely responsible for persisting
Kafka events to ObservatoryEvent.
"""

station = Stations.GBT
tracer = trace.get_tracer(f"{Stations(station).name.lower()}.kafka.consumer")

def process_msg(
    msg,
    producer_topic,
    producer_config,
):
    carrier = {}
    
    for name, value in (msg.headers() or []):
        if value is not None:
            carrier[name] = value.decode("utf-8")

    parent_context = propagate.extract(carrier)

    span = tracer.start_span(
        f"process message from {msg.topic()}",
        context=parent_context,
        kind=SpanKind.CONSUMER,
        attributes={
            "messaging.system": "kafka",
            "messaging.destination.name": msg.topic(),
            "messaging.operation.name": "process",
            "messaging.kafka.partition": msg.partition(),
            "messaging.kafka.offset": msg.offset(),
            "messaging.consumer.group.name": "gbt-consumer-group",
        },
    )

    # Make this span the active parent in Python's execution context
    ctx = trace.set_span_in_context(span)
    token = attach(ctx)

    # Performing our business logic within the span
    try:
            
        incoming_key = msg.key().decode("utf-8")

        # GBT only reacts to waveform requests
        # submitted by the UI.
        if (incoming_key!= str(Message.UI_EVENT.value)):
            return True

        span.set_attribute("ngradar.message.name", "UI_EVENT")

        payload = json.loads(msg.value().decode("utf-8"))

        waveform = payload["tx_waveform"]
        waveform_requester = payload["waveform_requester"]

        ui_event_time = (
            datetime.fromisoformat(
                payload["event_time"]
            )
        )

        print(
            "GBT received waveform request: "
            f"{waveform}"
        )

        # One ID correlates both the OFF and ON
        # events with the same observation.
        gbt_uuid = uuid.uuid4()

        span.set_attribute("ngradar.gbt_uuid", str(gbt_uuid))

        # -------------------------------------------------
        # 1. Turn transmitter OFF
        # -------------------------------------------------
        with tracer.start_as_current_span(
                "GBT transmitter OFF",
                attributes={
                    "ngradar.gbt_uuid": str(gbt_uuid),
                    "ngradar.transmitter.state": "OFF",
                },
        ):
            print("GBT transmitter OFF")
            send_kafka_message(
                message_type=(Message.STATUS_UPDATE),
                producer_topic=producer_topic,
                producer_config=producer_config,
                waveform_requester=waveform_requester,
                station=Stations.GBT,
                gbt_uuid=gbt_uuid,
                object_id="30104",
                target="Moretus",
                tx_waveform="TX_OFF",
                rec_waveform="TX_OFF",
                status=None,
                xmit_station=Stations.GBT,
                rcvr_station=None,
                latency_ms=latency_calc(
                    ui_event_time,
                    Stations.GBT,
                ),
                message=(
                    "GBT transmitter turned OFF "
                    "for waveform change."
                ),
            )

        # -------------------------------------------------
        # 2. Remain OFF for five seconds
        # -------------------------------------------------

        #time.sleep(5)

        # -------------------------------------------------
        # 3. Turn transmitter ON with new waveform
        # -------------------------------------------------
        with tracer.start_as_current_span(
                "GBT transmitter ON",
                attributes={
                    "ngradar.gbt_uuid": str(gbt_uuid),
                    "ngradar.transmitter.state": "ON",
                    "ngradar.waveform": str(waveform),
                },
        ):
            gbt_event_time = datetime.now(timezone.utc)
            print(f"GBT transmitter ON with waveform {waveform}")
            event_uuid = send_kafka_message(
                message_type=(Message.GBT_TX),
                producer_topic=producer_topic,
                producer_config=producer_config,
                waveform_requester=waveform_requester,
                station=Stations.GBT,
                gbt_uuid=gbt_uuid,
                gbt_event_time=(gbt_event_time.isoformat()),
                object_id="30104",
                target="Moretus",
                tx_waveform=waveform,
                rec_waveform=waveform,
                status=None,
                xmit_station=Stations.GBT,
                rcvr_station=None,
                latency_ms=latency_calc(
                    ui_event_time,
                    Stations.GBT,
                ),
                message=(
                    "GBT transmitting waveform "
                    f"{waveform}."
                ),
            )

        print(
            "GBT published TX event "
            f"{event_uuid}"
        )

        return True

    # Catch any unexpected exceptions and record them in the span
    except Exception as exc:
        span.record_exception(exc)
        span.set_status(TraceStatus(StatusCode.ERROR, str(exc)))
        # span.end() # Safely closing the span in case it wasn't already closed in the business logic above
        raise

    finally:
        detach(token) # Detach the context to avoid leaking it to other spans
        span.end()


class Command(BaseCommand):
    help = "Runs the GBT simulator"

    def handle(
        self,
        *args,
        **options,
    ):
        print("Starting GBT simulator")

        # provider = TracerProvider(sampler=ALWAYS_ON)
        provider = TracerProvider(
            sampler=ALWAYS_ON,
            resource=Resource.create({"service.name": "gbt"}),
        )
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://otel-collector:4317")) # endpoint will not work on droplets - configuring that later
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

        (
            producer_topic,
            producer_config,
            consumer_topic,
            consumer_config,
        ) = bootstrap(Stations.GBT)

        consume(
            consumer_topic,
            consumer_config,
            process_msg,
            producer_topic=producer_topic,
            producer_config=producer_config,
            manual_commit=True,
        )