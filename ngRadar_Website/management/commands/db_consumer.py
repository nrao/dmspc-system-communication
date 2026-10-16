import json, os
from ngRadar_Website.enums import Message, UIEvent
from ngRadar_Website.models.models import ObservatoryEvent

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter  # or HTTP
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry import trace, propagate
from opentelemetry.trace import SpanKind, StatusCode
from opentelemetry.trace.status import Status as TraceStatus
from opentelemetry.context import attach, detach

from ngRadar_Website.utils import (
    MAX_BYTES,
    consume,
    produce,
)
from django.db import transaction
from django.core.management.base import BaseCommand

"""
DB Consumner

This simulator:

- Consumes every Kafka message in the cluster.
- Saves the message to the database as an ObservatoryEvent.

The db_consumer is solely responsible for persisting
Kafka events to ObservatoryEvent.
"""
station = "DB_CONSUMER"
tracer = trace.get_tracer(f"db_consumer.kafka")


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
            "messaging.consumer.group.name": os.getenv("DB_KAFKA_GROUP_ID", "ngradar-db"),
        },
    )

    # Make this span the active parent in Python's execution context
    ctx = trace.set_span_in_context(span)
    token = attach(ctx)

    # Performing our business logic within the span
    try:
        incoming_key = msg.key().decode("utf-8")

        # Do not consume our own
        # database acknowledgements when we produce back into dsoc_notif.
        if incoming_key == str(Message.DB_COMMITTED.value):
            return True
        if incoming_key == UIEvent.IMAGE_CHANGED:
            return True

        try:
            span.set_attribute("ngradar.message.name", Message(int(incoming_key)).name)
        except (ValueError, TypeError):
            span.set_attribute("ngradar.message.name", incoming_key)

        topic = msg.topic()

        with tracer.start_as_current_span("decode DB event"):
            payload = json.loads(msg.value().decode("utf-8"))

        for key in ("event_uuid", "gbt_uuid", "transfer_uuid", "station", "target"):
            if payload.get(key) is not None:
                span.set_attribute(f"ngradar.{key}", str(payload[key]))

        with tracer.start_as_current_span("persist DB event transaction"), transaction.atomic():
            event, created = record_obs_event(
                payload
            )

            # Capture values needed for the UI event
            event_uuid = str(event.uuid)
            image_key = event.image_key
            rcvr_station = event.rcvr_station

            transaction.on_commit(
                lambda: publish_db_committed(
                    topic=topic,
                    producer_config=producer_config,
                    payload=payload,
                    parent_context=ctx,
                )
            )

            # Only publish image_changed SSE event type when
            # this event actually has an image.
            if image_key:
                transaction.on_commit(
                    lambda: publish_ui_event(
                        topic=topic,
                        producer_config=producer_config,
                        parent_context=ctx,
                        event_type=UIEvent.IMAGE_CHANGED,
                        key=event_uuid,
                        data={
                            "event_uuid": event_uuid,
                            "rcvr_station": rcvr_station,
                        },
                    )
                )

        return True


    except json.JSONDecodeError as error:
        span.record_exception(error)
        span.set_status(TraceStatus(StatusCode.ERROR, str(error)))
        print(
            "DB consumer received "
            "invalid JSON: "
            f"{error}"
        )
        return False

    except (
            KeyError,
            TypeError,
            ValueError,
    ) as error:
        span.record_exception(error)
        span.set_status(TraceStatus(StatusCode.ERROR, str(error)))
        print(
            "DB consumer received "
            "invalid payload: "
            f"{error}"
        )
        return False

    except Exception as exc:
        print(f"DB consumer failed to process message: {exc}")

        span.record_exception(exc)
        span.set_status(TraceStatus(StatusCode.ERROR, str(exc)))
        raise

    finally:
        detach(token)  # Detach the context to avoid leaking it to other spans
        span.end()


def record_obs_event(payload):
    with tracer.start_as_current_span("upsert ObservatoryEvent") as db_span:
        obs_event, created = (
            ObservatoryEvent.objects.update_or_create(
                uuid=payload["event_uuid"],
                defaults={
                    "gbt_uuid": payload.get("gbt_uuid"),
                    "transfer_uuid": payload.get("transfer_uuid"),
                    "object_id": payload.get("object_id"),
                    "target": payload.get("target"),
                    "waveform_requester": payload.get("waveform_requester"),
                    "tx_waveform": payload.get("tx_waveform"),
                    "rec_waveform": payload.get("rec_waveform"),
                    "product_type": payload.get("product_type"),
                    "product_id": payload.get("product_id"),
                    "station": payload.get("station"),
                    "event_time": payload["event_time"],
                    "xmit_station": payload.get("xmit_station"),
                    "rcvr_station": payload.get("rcvr_station"),
                    "image_key": payload.get("image_key"),
                    "num_bytes": payload.get("num_bytes"),
                    "latency_ms": payload.get("latency_ms", 0.0, ),
                    "status": payload.get("status"),
                    "message": payload.get("message", "", ),
                },
            )
        )
        db_span.set_attribute("ngradar.db.created", created)

    return obs_event, created


def publish_db_committed(
        *,
        topic,
        producer_config,
        payload,
        parent_context=None,
):
    notification = {
        "event_type": "db_committed",
        "data": payload,
    }

    with tracer.start_as_current_span("publish DB committed", context=parent_context):
        produce(
            topic,
            producer_config,
            str(
                Message.DB_COMMITTED.value
            ),
            json.dumps(notification),
            station=station,
        )


def publish_ui_event(
        *,
        topic,
        producer_config,
        event_type,
        key,
        data,
        parent_context=None,
):
    notification = {
        "event_type": event_type,
        "key": key,
        "data": data,
    }

    with tracer.start_as_current_span("publish image changed", context=parent_context):
        produce(
            topic,
            producer_config,
            event_type,
            json.dumps(notification),
        )


class Command(BaseCommand):
    help = "Consume Kafka domain events and persist them to ObservatoryEvent."

    def handle(self, *args, **options):
        print("Starting DB consumer")

        provider = TracerProvider(
            sampler=ALWAYS_ON,
            resource=Resource.create({"service.name": "db_consumer"}),
        )
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://otel-collector:4317"))
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

        bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

        topics = [
            topic.strip()
            for topic in os.getenv(
                "DB_KAFKA_TOPICS",
                (
                    "GBT_notif,"
                    "VLBA_notif,"
                    "DSOC_notif"
                ),
            ).split(",")
            if topic.strip()
        ]

        consumer_config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": os.getenv(
                "DB_KAFKA_GROUP_ID",
                "ngradar-db",
            ),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }

        producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "message.max.bytes": MAX_BYTES,
            "message.timeout.ms": 2000,
            "client.id":
                "db-consumer-producer",
        }

        self.stdout.write(
            self.style.SUCCESS(
                "DB consumer starting\n"
                f"  brokers: {bootstrap_servers}\n"
                f"  topics: {topics}\n"
                "  group: ngradar-db"
            )
        )

        consume(
            topics,
            consumer_config,
            process_msg,
            producer_config=producer_config,
            manual_commit=True,
        )