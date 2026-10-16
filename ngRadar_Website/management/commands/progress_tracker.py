"""Track incoming e-transfers and notify DSOC when they complete.

The db_consumer owns persistence of workflow events.
"""
import json
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError
from django.core.management.base import BaseCommand
from opentelemetry import propagate, trace
from opentelemetry.context import attach, detach
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.trace import SpanKind, StatusCode
from opentelemetry.trace.status import Status as TraceStatus

from ngRadar_Website.enums import Message, Stations, Status
from ngRadar_Website.utils import (
    bootstrap,
    consumer_group_has_members,
    produce,
    send_kafka_message,
)

STALL_TIMEOUT_SECONDS = 15
volume_folder = Path("/dsoc/incoming")
station = Stations.PTW
tracer = trace.get_tracer(f"{Stations(station).name.lower()}.kafka.consumer")


def publish_progress(*, producer_config, payload):
    return produce(
        "progress_tracking",
        producer_config,
        str(Message.PROGRESS_UPDATE.value),
        json.dumps(payload),
        station=station,
    )


def process_msg(msg, active_transfers, producer_topic, producer_config):
    carrier = {
        name: value.decode("utf-8")
        for name, value in (msg.headers() or [])
        if value is not None
    }
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
            "messaging.consumer.group.name": f"{Stations(station).name.lower()}-consumer-group",
        },
    )
    token = attach(trace.set_span_in_context(span))
    try:
        incoming_key = msg.key().decode("utf-8")
        payload = json.loads(msg.value().decode("utf-8"))
        if incoming_key != str(Message.VLBA_TRANSFERRING.value):
            return True

        span.set_attribute("ngradar.message.name", "VLBA_TRANSFERRING")
        span.set_attribute("ngradar.transfer_uuid", str(payload["transfer_uuid"]))
        payload["last_progress_at"] = time.monotonic()
        payload["tracking_started_at"] = payload["last_progress_at"]
        payload["last_received_bytes"] = 0
        # This is process-local tracking state, never serialized to Kafka.
        # Subsequent progress spans join the trace after this consumer span ends.
        payload["_trace_context"] = trace.set_span_in_context(span)
        active_transfers.append(payload)
        return True
    except Exception as exc:
        span.record_exception(exc)
        span.set_status(TraceStatus(StatusCode.ERROR, str(exc)))
        raise
    finally:
        detach(token)
        span.end()


def progress_consume(topic, config, producer_topic=None, producer_config=None, manual_commit=False):
    """Consume transfer-start messages and check active transfers between polls."""
    if manual_commit:
        config = {**config, "enable.auto.commit": False}

    consumer = Consumer(config)
    consumer.subscribe(topic)
    active_transfers = []
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    error = msg.error()
                    if error.code() != KafkaError._PARTITION_EOF:
                        print("Consumer error:", error)
                        break
                    print("Consumer reached partition EOF.")
                else:
                    succeeded = process_msg(
                        msg, active_transfers, producer_topic, producer_config
                    )
                    if manual_commit and succeeded:
                        consumer.commit(msg)

            # Check after every poll, including when Kafka traffic is continuous.
            completed_transfers = []
            for transfer in active_transfers:
                if get_transfer_progress(transfer, producer_topic, producer_config):
                    completed_transfers.append(transfer)
            for transfer in completed_transfers:
                active_transfers.remove(transfer)
    finally:
        consumer.close()


def get_transfer_progress(payload, producer_topic, producer_config):
    """Check an incoming file and emit spans only for meaningful changes."""
    filename = payload["filename"]
    transfer_uuid = payload["transfer_uuid"]
    vlba_station_id = payload["station"]
    last_progress_at = payload["last_progress_at"]
    last_received_bytes = payload["last_received_bytes"]
    tracked_file = volume_folder / filename
    total_bytes = int(payload["num_bytes"])
    tracking_context = payload["_trace_context"]
    span_attributes = {
        "ngradar.transfer_uuid": str(transfer_uuid),
        "ngradar.vlba.station": Stations(vlba_station_id).name,
    }

    if total_bytes <= 0:
        with tracer.start_as_current_span(
            "e-transfer tracking error", context=tracking_context,
            attributes=span_attributes,
        ):
            raise ValueError("Expected transfer size must be greater than zero.")

    if not tracked_file.exists():
        return False

    current_bytes = tracked_file.stat().st_size
    if current_bytes > last_received_bytes:
        last_progress_at = time.monotonic()
        percent = min(100.0, current_bytes / total_bytes * 100)
        with tracer.start_as_current_span(
            "e-transfer progress update", context=tracking_context,
            attributes={
                **span_attributes,
                "ngradar.transfer.received_bytes": current_bytes,
                "ngradar.transfer.total_bytes": total_bytes,
                "ngradar.transfer.percent": round(percent, 2),
            },
        ):
            delivered = publish_progress(
                producer_config=producer_config,
                payload={
                    "gbt_uuid": payload["gbt_uuid"],
                    "transfer_uuid": transfer_uuid,
                    "station": vlba_station_id,
                    "station_name": Stations(vlba_station_id).name,
                    "received_bytes": current_bytes,
                    "total_bytes": total_bytes,
                    "percent": round(percent, 2),
                },
            )
            if delivered is False:
                raise RuntimeError("Failed to publish e-transfer progress.")
        print(
            f"Transfer {transfer_uuid} from VLBA-{Stations(vlba_station_id).name}: "
            f"{percent:.2f}% ({current_bytes}/{total_bytes})"
        )

    last_received_bytes = current_bytes
    if current_bytes >= total_bytes:
        elapsed_seconds = time.monotonic() - payload["tracking_started_at"]
        with tracer.start_as_current_span(
            "e-transfer completed", context=tracking_context,
            attributes={
                **span_attributes,
                "ngradar.transfer.received_bytes": current_bytes,
                "ngradar.transfer.total_bytes": total_bytes,
                "ngradar.transfer.elapsed_seconds": elapsed_seconds,
            },
        ):
            print(f"Transfer of <{transfer_uuid}.bin> COMPLETE.")
            send_kafka_message(
                producer_topic=producer_topic,
                producer_config=producer_config,
                waveform_requester=payload["waveform_requester"],
                message_type=Message.PROGRESS_COMPLETE,
                transfer_uuid=transfer_uuid,
                gbt_uuid=payload["gbt_uuid"],
                gbt_event_time=payload["gbt_event_time"],
                station=Stations(vlba_station_id),
                status=Status.TRANSFERRED,
                object_id=payload["object_id"],
                target=payload["target"],
                tx_waveform=payload["tx_waveform"],
                rec_waveform=payload["rec_waveform"],
                num_bytes=payload["num_bytes"],
                filename=payload["filename"],
                xmit_station=payload["xmit_station"],
                rcvr_station=payload["rcvr_station"],
                message=(
                    f"VLBA-{Stations(vlba_station_id).name} completed sending "
                    "the data file to DSOC via e-transfer."
                ),
            )
        return True

    if time.monotonic() - last_progress_at > STALL_TIMEOUT_SECONDS:
        vlba_station = Stations(vlba_station_id)
        vlba_consumer_group = f"{vlba_station.name.lower()}-consumer-group"
        with tracer.start_as_current_span(
            "e-transfer stall check", context=tracking_context,
            attributes={**span_attributes, "ngradar.transfer.received_bytes": current_bytes},
        ) as stall_span:
            vlba_alive = consumer_group_has_members(vlba_consumer_group)
            stall_span.set_attribute("ngradar.vlba.consumer_alive", vlba_alive)
            if vlba_alive:
                last_progress_at = time.monotonic()
            else:
                raise RuntimeError(
                    f"{vlba_station.label} went offline mid-transfer. Transfer interrupted."
                )

    payload["last_progress_at"] = last_progress_at
    payload["last_received_bytes"] = last_received_bytes
    return False


class Command(BaseCommand):
    help = "Runs the e-transfer progress tracking simulator"

    def handle(self, *args, **options):
        print("Starting e-transfer progress tracking simulator")
        provider = TracerProvider(
            sampler=ALWAYS_ON,
            resource=Resource.create({"service.name": "progress_tracker"}),
        )
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://otel-collector:4317"))
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

        producer_topic, producer_config, consumer_topic, consumer_config = bootstrap(Stations.PTW)
        progress_consume(
            consumer_topic,
            consumer_config,
            producer_topic=producer_topic,
            producer_config=producer_config,
            manual_commit=True,
        )
