from datetime import datetime

import io
import json
import os
import time
import uuid

from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

from django.core.management.base import BaseCommand

from ngRadar_Website.enums import Stations, Status, Message

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter # or HTTP
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry import trace, propagate
from opentelemetry.trace import SpanKind, StatusCode
from opentelemetry.trace.status import Status as TraceStatus
from opentelemetry.context import attach, detach

from ngRadar_Website.utils import (
    bootstrap,
    consume,
    consumer_group_has_members,
    create_s3_client,
    get_folder_size,
    latency_calc,
    send_kafka_message,
    upload_seaweedfs,
    delete_observation_data,
)


"""
DSOC Simulator

This simulator:

- Consumes VLBA workflow events from Kafka.
- Checks DSOC storage availability.
- Consumes notification that expedat is complete.
- Verifies the completed transfer.
- Generates the simulated DDM product.
- Stores the DDM image in SeaweedFS.
- Sends DSOC state/workflow events to Kafka.

This simulator does NOT write directly to the database (the ObservatoryEvent table).

The db_consumer is solely responsible for persisting
Kafka events to ObservatoryEvent.
"""


STALL_TIMEOUT_SECONDS = 15

MAX_STORAGE_RETRIES = 15

station = Stations.DSOC
tracer = trace.get_tracer(f"{Stations(station).name.lower()}.kafka.consumer")

# =============================================================
# DDM IMAGE GENERATION
# =============================================================

def create_img(station, tx_waveform, waveform_requester):
    """Generate a simulated DSOC DDM product."""
    with tracer.start_as_current_span("generate DDM") as image_span:
        matplotlib.use("Agg")

        x_data = np.random.uniform(-30, 30, 40)
        y_data = np.random.uniform(-300, 300, 40)
        plt.scatter(x_data, y_data, color="red")
        plt.axhline(0, color="black", linewidth=0.5)
        plt.axvline(0, color="black", linewidth=0.5)
        plt.suptitle(
            f"[Station {Stations(station).name}] DDM for {tx_waveform}",
            size=20,
        )
        plt.title(f"Requested by {waveform_requester}")
        plt.xlabel("Doppler Freq (Hz)")
        plt.ylabel("Range (km)")
        plt.grid(True)

        byte_buffer = io.BytesIO()
        try:
            plt.savefig(byte_buffer, format="png")
            image_file = byte_buffer.getvalue()
        finally:
            plt.close()
            byte_buffer.close()

        image_span.set_attribute("ngradar.image.bytes", len(image_file))
        return image_file, len(image_file)

# =============================================================
# SEAWEEDFS
# =============================================================

def save_image_to_seaweedfs(target, image_file, product_uuid):
    """Save the generated DDM image to SeaweedFS."""
    image_key = f"ddm/{target}/{product_uuid}.png"

    with tracer.start_as_current_span(
        "upload DDM to SeaweedFS",
        attributes={
            "ngradar.image.bytes": len(image_file),
            "ngradar.target": str(target),
        },
    ) as upload_span:
        try:
            s3 = create_s3_client(station=Stations.DSOC)
            image_key = upload_seaweedfs(s3, image_key, image_file)
            upload_span.set_attribute("ngradar.image.key", image_key)

            print(f"Success: Image saved to SeaweedFS at {image_key}")
            return image_key

        except Exception as exc:
            upload_span.record_exception(exc)
            upload_span.set_status(
                TraceStatus(StatusCode.ERROR, str(exc))
            )
            raise RuntimeError(
                "Failed to save DDM image to SeaweedFS."
            ) from exc


# =============================================================
# TRANSFER VERIFICATION
# =============================================================

def verify_incoming_transfer(
    *,
    incoming_file,
    expected_num_bytes,
    producer_topic,
    producer_config,
    waveform_requester,
    gbt_event_time,
    gbt_uuid,
    object_id,
    target,
    tx_waveform,
    rec_waveform,
    filename,
    transfer_uuid,
    vlba_station,
    attempts=10,
    delay_seconds=0.5,
):
    """
    Verify that the incoming file exists, matches the
    expected byte count supplied by VLBA, and sends a
    kafka message if successful.
    """
    expected_num_bytes = int(expected_num_bytes)

    with tracer.start_as_current_span(
        "verify incoming transfer",
        attributes={
            "ngradar.transfer_uuid": str(transfer_uuid),
            "ngradar.expected_bytes": expected_num_bytes,
            "ngradar.max_attempts": attempts,
        },
    ) as verify_span:
        for attempt in range(1, attempts + 1):
            verify_span.set_attribute("ngradar.attempts", attempt)

            if incoming_file.is_file():
                actual_num_bytes = incoming_file.stat().st_size
                verify_span.set_attribute(
                    "ngradar.actual_bytes", actual_num_bytes
                )

                if actual_num_bytes == expected_num_bytes:
                    send_kafka_message(
                        producer_topic=(producer_topic),
                        producer_config=(producer_config),
                        waveform_requester=waveform_requester,
                        message_type=(Message.STATUS_UPDATE),
                        transfer_uuid=(transfer_uuid),
                        gbt_uuid=gbt_uuid,
                        gbt_event_time=(gbt_event_time),
                        station=Stations.DSOC,
                        status=Status.VERIFIED,
                        object_id=object_id,
                        target=target,
                        tx_waveform=tx_waveform,
                        rec_waveform=(rec_waveform),
                        num_bytes=expected_num_bytes,
                        filename=filename,
                        xmit_station=(Stations.GBT),
                        rcvr_station=vlba_station,
                        message=(
                            f"Verified incoming transfer of "
                            f"{filename} with "
                            f"{actual_num_bytes} bytes."
                        ),
                    )
                    return actual_num_bytes

                time.sleep(delay_seconds)

        raise RuntimeError(
            f"Transfer verification failed for {incoming_file}. "
            f"Expected {expected_num_bytes} bytes."
        )

# =============================================================
# KAFKA PROCESSING
# =============================================================

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
            "messaging.consumer.group.name": "dsoc-consumer-group",
        },
    )

    # Make this span the active parent in Python's execution context
    ctx = trace.set_span_in_context(span)
    token = attach(ctx)

    # Performing our business logic within the span
    try:

        incoming_key = int(
            msg.key().decode("utf-8")
        )

        # DB_COMMITTED is intended for the website/UI consumer.
        if (incoming_key == Message.DB_COMMITTED.value):
            return True

        # Generic status events do not instruct DSOC
        # to perform workflow actions.
        if (incoming_key == Message.STATUS_UPDATE.value):
            return True

        payload = json.loads(
            msg.value().decode("utf-8")
        )

        span.set_attributes({
            "ngradar.station": "DSOC",
            "ngradar.message.type": incoming_key,
            "ngradar.message.name": Message(incoming_key).name,
        })

        volume_folder = Path("/dsoc/incoming")

        # ---------------------------------------------------------
        # Context propagated from GBT -> VLBA -> DSOC
        # ---------------------------------------------------------

        transfer_uuid = payload.get("transfer_uuid")
        if transfer_uuid:
            span.set_attribute(
                "ngradar.transfer_uuid",
                str(transfer_uuid),
            )

        gbt_uuid = payload.get("gbt_uuid")

        object_id = payload.get("object_id")

        target = payload.get("target")
        if target:
            span.set_attribute(
                "ngradar.target",
                target,
            )

        waveform_requester = payload.get("waveform_requester")

        tx_waveform = payload.get("tx_waveform")

        rec_waveform = payload.get("rec_waveform")

        gbt_event_time = payload.get("gbt_event_time")

        filename = payload.get("filename")

        num_bytes = int(
            payload.get(
                "num_bytes",
                0,
            )
        )

        station = payload.get("station")

        vlba_station = Stations(station)

        retry_count = int(
            payload.get(
                "retry_count",
                0,
            )
        )

        # =========================================================
        # VLBA -> DSOC
        #
        # VLBA requests a storage check.
        # =========================================================

        if (incoming_key == Message.VLBA_REQUEST_STORAGE.value):
            expected_num_bytes = (num_bytes)

            with tracer.start_as_current_span(
                    "check DSOC storage"
            ) as storage_span:
                storage_limit = (int(os.environ["DSOC_VOLUME_SIZE"]) * 1_000_000_000)
                storage_used = int(get_folder_size(volume_folder))
                space_remaining = storage_limit - storage_used
                has_space = (storage_used + expected_num_bytes < storage_limit)

                storage_span.set_attributes({
                    "ngradar.storage.limit_bytes": storage_limit,
                    "ngradar.storage.used_bytes": storage_used,
                    "ngradar.storage.remaining_bytes": space_remaining,
                    "ngradar.storage.requested_bytes": expected_num_bytes,
                    "ngradar.storage.has_space": has_space,
                })

            print(
                "DSOC has "
                f"{space_remaining / 1_000_000_000:0.2f}"
                "GB of storage remaining."
            )

            # -----------------------------------------------------
            # NOT ENOUGH STORAGE
            # -----------------------------------------------------

            if (storage_used + expected_num_bytes >= storage_limit):
                next_retry_count = (retry_count + 1)

                # -------------------------------------------------
                # Maximum retries reached
                # -------------------------------------------------

                if (next_retry_count >= MAX_STORAGE_RETRIES):
                    send_kafka_message(
                        producer_topic=(producer_topic),
                        producer_config=(producer_config),
                        waveform_requester=waveform_requester,
                        message_type=(Message.DSOC_RESPOND_STORAGE),
                        transfer_uuid=(transfer_uuid),
                        gbt_uuid=gbt_uuid,
                        gbt_event_time=(gbt_event_time),
                        station=Stations.DSOC,
                        status=Status.FAILED,
                        object_id=object_id,
                        target=target,
                        tx_waveform=(tx_waveform),
                        rec_waveform=(rec_waveform),
                        num_bytes=(expected_num_bytes),
                        filename=filename,
                        retry_count=(next_retry_count),
                        xmit_station=(Stations.GBT),
                        rcvr_station=(vlba_station),
                        message=(
                            f"{vlba_station.name} requested a storage check at DSOC. "
                            f"DSOC responded that it does not have enough storage and cannot begin the expedat stream."
                            f"Failed after "
                            f"{next_retry_count} "
                            "storage checks."
                        ),
                    )

                    print(
                        "DSOC failed to clear "
                        "enough storage after "
                        f"{next_retry_count} "
                        "tries."
                    )

                    return True

                # -------------------------------------------------
                # Ask VLBA to retry later.
                #
                # This message is also persisted by db_consumer
                # as a DSOC RETRYING event.
                # -------------------------------------------------

                send_kafka_message(
                    producer_topic=(producer_topic),
                    producer_config=(producer_config),
                    waveform_requester=waveform_requester,
                    message_type=(Message.DSOC_RESPOND_STORAGE),
                    transfer_uuid=(transfer_uuid),
                    gbt_uuid=gbt_uuid,
                    gbt_event_time=(gbt_event_time),
                    station=Stations.DSOC,
                    status=Status.RETRYING,
                    object_id=object_id,
                    target=target,
                    tx_waveform=tx_waveform,
                    rec_waveform=(rec_waveform),
                    num_bytes=(expected_num_bytes),
                    filename=filename,
                    retry_count=(next_retry_count),
                    xmit_station=(Stations.GBT),
                    rcvr_station=(vlba_station),

                    message=f"{vlba_station.name} requested a storage check at DSOC. DSOC responded that it does not have enough storage and cannot begin the expedat stream.",
                )

                print(
                    "DSOC does not have enough "
                    "storage to accept the "
                    "transfer request. "
                    "Remaining disk space: "
                    f"{space_remaining / 1_000_000_000:0.2f}"
                    "GB. Incoming data: "
                    f"{expected_num_bytes / 1_000_000_000:0.2f}"
                    "GB."
                )

            # -----------------------------------------------------
            # ENOUGH STORAGE
            # -----------------------------------------------------

            else:
                send_kafka_message(
                    producer_topic=(producer_topic),
                    producer_config=(producer_config),
                    waveform_requester=waveform_requester,
                    message_type=(Message.DSOC_RESPOND_STORAGE),
                    transfer_uuid=(transfer_uuid),
                    gbt_uuid=gbt_uuid,
                    gbt_event_time=(gbt_event_time),
                    station=Stations.DSOC,
                    status=Status.READY,
                    object_id=object_id,
                    target=target,
                    tx_waveform=tx_waveform,
                    rec_waveform=(rec_waveform),
                    num_bytes=(expected_num_bytes),
                    filename=filename,
                    retry_count=(retry_count),
                    xmit_station=(Stations.GBT),
                    rcvr_station=(vlba_station),
                    message=f"DSOC responded that it has enough storage. {vlba_station.name} may begin the expedat stream.",
                )

                print(
                    "DSOC has enough storage "
                    "to accept the incoming "
                    "data. Awaiting "
                    "expedat stream..."
                )

        # =========================================================
        # VLBA -> DSOC
        #
        # VLBA has started the expedat stream.
        # =========================================================

        elif incoming_key == Message.PROGRESS_COMPLETE.value:

            incoming_file = volume_folder / f"{transfer_uuid}.bin"

            # -----------------------------------------------------
            # DSOC begins verification
            # -----------------------------------------------------

            send_kafka_message(
                producer_topic=(producer_topic),
                producer_config=(producer_config),
                waveform_requester=waveform_requester,
                message_type=(Message.STATUS_UPDATE),
                transfer_uuid=(transfer_uuid),
                gbt_uuid=gbt_uuid,
                gbt_event_time=(gbt_event_time),
                station=Stations.DSOC,
                status=Status.VERIFYING,
                object_id=object_id,
                target=target,
                tx_waveform=tx_waveform,
                rec_waveform=(rec_waveform),
                num_bytes=num_bytes,
                filename=filename,
                xmit_station=(Stations.GBT),
                rcvr_station=(vlba_station),
                message=(
                    f"Verifying "
                    f"{filename}."
                ),
            )

            # -----------------------------------------------------
            # Verify incoming file
            # -----------------------------------------------------

            try:
                actual_num_bytes = verify_incoming_transfer(
                    incoming_file=incoming_file,
                    expected_num_bytes=num_bytes,

                    producer_topic=producer_topic,
                    producer_config=producer_config,
                    waveform_requester=waveform_requester,

                    gbt_event_time=gbt_event_time,
                    gbt_uuid=gbt_uuid,

                    object_id=object_id,
                    target=target,

                    tx_waveform=tx_waveform,
                    rec_waveform=rec_waveform,

                    filename=filename,
                    transfer_uuid=transfer_uuid,
                    vlba_station=vlba_station,
                )

            except Exception as exc:
                send_kafka_message(
                    producer_topic=(producer_topic),
                    producer_config=(producer_config),
                    waveform_requester=waveform_requester,
                    message_type=(Message.STATUS_UPDATE),
                    transfer_uuid=(transfer_uuid),
                    gbt_uuid=gbt_uuid,
                    gbt_event_time=(gbt_event_time),
                    station=Stations.DSOC,
                    status=Status.FAILED,
                    object_id=object_id,
                    target=target,
                    tx_waveform=tx_waveform,
                    rec_waveform=(rec_waveform),
                    num_bytes=0,
                    filename=filename,
                    xmit_station=(Stations.GBT),
                    rcvr_station=(vlba_station),
                    message=str(exc),
                )

                span.record_exception(exc)
                span.set_status(
                    TraceStatus(StatusCode.ERROR, str(exc))
                )

                return True

            # -----------------------------------------------------
            # Generate and store DDM
            # -----------------------------------------------------

            try:
                if not gbt_event_time:
                    raise ValueError(
                        "GBT event time is "
                        "missing from the "
                        "Kafka payload."
                    )

                original_gbt_time = (
                    datetime.fromisoformat(gbt_event_time)
                )

                dsoc_latency = (
                    latency_calc(
                        original_gbt_time,
                        Stations.DSOC,
                    )
                )

                image_file, image_num_bytes = (
                    create_img(
                        station,
                        tx_waveform,
                        waveform_requester=waveform_requester
                    )
                )

                product_uuid = (uuid.uuid4())

                image_key = (
                    save_image_to_seaweedfs(
                        target,
                        image_file,
                        product_uuid,
                    )
                )

            except Exception as exc:
                print(
                    "DSOC image processing "
                    f"failed: {exc}"
                )

                send_kafka_message(
                    producer_topic=(producer_topic),
                    producer_config=(producer_config),
                    waveform_requester=waveform_requester,
                    message_type=(Message.STATUS_UPDATE),
                    transfer_uuid=(transfer_uuid),
                    gbt_uuid=gbt_uuid,
                    gbt_event_time=(gbt_event_time),
                    station=Stations.DSOC,
                    status=Status.FAILED,
                    object_id=object_id,
                    target=target,
                    tx_waveform=tx_waveform,
                    rec_waveform=(rec_waveform),
                    num_bytes=(actual_num_bytes),
                    filename=filename,
                    xmit_station=(Stations.GBT),
                    rcvr_station=(vlba_station),
                    message=(
                        "DSOC image processing "
                        f"failed: {exc}"
                    ),
                )

                span.record_exception(exc)
                span.set_status(
                    TraceStatus(StatusCode.ERROR, str(exc))
                )

                return True

            # -----------------------------------------------------
            # COMPLETE
            #
            # This single event:
            #
            # 1. records DSOC COMPLETED
            # 2. includes the generated DDM product
            # 3. tells VLBA to delete its raw data
            # -----------------------------------------------------

            send_kafka_message(
                producer_topic=(producer_topic),
                producer_config=(producer_config),
                waveform_requester=waveform_requester,
                message_type=(Message.VLBA_DELETE),
                transfer_uuid=(transfer_uuid),
                gbt_uuid=gbt_uuid,
                gbt_event_time=(gbt_event_time),
                station=Stations.DSOC,
                status=Status.COMPLETED,
                object_id=object_id,
                target=target,
                tx_waveform=tx_waveform,
                rec_waveform=(rec_waveform),
                product_type="DDM",
                product_id=str(product_uuid),
                image_key=image_key,
                # The final DSOC product row describes
                # the generated DDM product size.
                num_bytes=(image_num_bytes),
                filename=filename,
                latency_ms=(dsoc_latency),
                xmit_station=(Stations.GBT),
                rcvr_station=(vlba_station),
                message=(
                    f"DSOC verified {vlba_station.label}'s "
                    "streaming, generated "
                    "the DDM image, stored "
                    "the image, and completed "
                    f"processing. {vlba_station.label} may "
                    "delete its raw data."
                ),
            )

            with tracer.start_as_current_span("delete incoming file"):
                delete_observation_data(
                    filename,
                    directory="/dsoc/incoming",
                )

            print(
                "DSOC processing COMPLETE."
            )

        else:
            print(
                "Invalid Kafka Message Key!"
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
    help = "Runs the DSOC simulator"

    def handle(
        self,
        *args,
        **options,
    ):
        print("Starting DSOC simulator")

        provider = TracerProvider(sampler=ALWAYS_ON)
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://otel-collector:4317"))
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

        (
            producer_topic,
            producer_config,
            consumer_topic,
            consumer_config,
        ) = bootstrap(
            Stations.DSOC
        )

        consume(consumer_topic,
                consumer_config,
                process_msg,
                producer_topic=producer_topic, producer_config=producer_config,
                manual_commit=True,
        )