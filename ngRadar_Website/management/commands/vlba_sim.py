import json
import subprocess
import time
import uuid
import os
from pathlib import Path
from threading import Thread

from django.core.management.base import BaseCommand

from ngRadar_Website.enums import Stations, Status, Message, UIEvent

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
    bootstrap,
    consume,
    create_file,
    watch_for_file,
    send_kafka_message,
    etc_send,
    wait_for_etd,
    delete_observation_data,
    ETD_MAX_CONN_RETRY,
    ETD_RETRY_CONN_DELAY,
)

"""
VLBA Simulator

This simulator:

- Consumes GBT_TX events from Kafka.
- Generates/stages VLBA observation data.
- Requests DSOC storage availability.
- Sends the data to DSOC using e-transfer.
- Publishes VLBA state changes to Kafka.
- Does NOT write ObservatoryEvent directly.

The db_consumer is responsible for consuming these Kafka
messages and persisting them to ObservatoryEvent.

Each container selects its VLBA site using STATION_NAME.
"""

FAILURE_REASONS = {
    -9: "The e-transfer process was terminated",
    -6: "The connection to the e-transfer daemon was lost",
}

MAX_RESUME_ATTEMPTS = 5

STATION = Stations[os.environ.get("STATION_NAME")]
tracer = trace.get_tracer(f"{Stations(STATION).name}.kafka.consumer")


def create_traced_file(frame_path, parent_context, attributes):
    # Threads do not automatically inherit the active OpenTelemetry context.
    with tracer.start_as_current_span(
            "generate VLBA raw data", context=parent_context, attributes=attributes,
    ):
        create_file(frame_path)


def tag_observation(span, payload):
    for key in ("gbt_uuid", "transfer_uuid", "target"):
        if payload.get(key) is not None:
            span.set_attribute(f"ngradar.{key}", str(payload[key]))


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
        f"process {msg.topic()}",
        context=parent_context,
        kind=SpanKind.CONSUMER,
        attributes={
            "ngradar.vlba.station": STATION.name,
            "messaging.system": "kafka",
            "messaging.destination.name": msg.topic(),
            "messaging.operation.name": "process",
            "messaging.kafka.partition": msg.partition(),
            "messaging.kafka.offset": msg.offset(),
            "messaging.consumer.group.name": f"{STATION.name.lower()}-consumer-group",
        },
    )

    # Make this span the active parent in Python's execution context
    ctx = trace.set_span_in_context(span)
    token = attach(ctx)

    # Performing our business logic within the span
    try:

        incoming_key = msg.key().decode("utf-8")

        if incoming_key.isdigit():
            try:
                span.set_attribute("ngradar.message.name", Message(int(incoming_key)).name)
            except ValueError:
                pass

        raw_data_path = Path("/raw_data")

        # ------------------------------------------------------------------
        # DB_COMMITTED + IMAGE_CHANGED + STATUS_UPDATE kafka messages
        # are acknowledgements intended for the website/UI consumers only.
        # VLBAs should ignore these.
        # ------------------------------------------------------------------
        if incoming_key == str(
                Message.DB_COMMITTED.value) or incoming_key == UIEvent.IMAGE_CHANGED or incoming_key == str(
                Message.STATUS_UPDATE.value):
            return True

        # =========================================================
        # GBT -> VLBA
        # =========================================================
        if incoming_key == str(Message.GBT_TX.value):
            print(
                "Received Kafka message from GBT."
            )

            payload = json.loads(msg.value().decode("utf-8"))
            tag_observation(span, payload)

            # An observation-wide correlation ID.
            gbt_uuid = payload["gbt_uuid"]
            station = payload["station"]

            # Preserve the original GBT timestamp for
            # end-to-end latency calculation at DSOC.
            gbt_event_time = payload.get(
                "gbt_event_time",
                payload["event_time"],
            )

            # Observation context inherited from GBT.
            object_id = payload.get("object_id")

            target = payload.get("target")

            waveform_requester = payload.get("waveform_requester")

            tx_waveform = payload.get("tx_waveform")

            rec_waveform = payload.get("rec_waveform")

            # One transfer UUID identifies this entire
            # VLBA -> DSOC e-transfer lifecycle.
            transfer_uuid = uuid.uuid4()

            span.set_attribute("ngradar.transfer_uuid", str(transfer_uuid))

            frame_path = (raw_data_path / f"{transfer_uuid}.bin")

            Thread(
                target=create_traced_file,
                args=(frame_path, trace.set_span_in_context(span), {
                    "ngradar.vlba.station": STATION.name,
                    "ngradar.transfer_uuid": str(transfer_uuid),
                    "ngradar.gbt_uuid": str(gbt_uuid),
                }),
                daemon=True,
            ).start()

            with tracer.start_as_current_span("wait for VLBA raw data"):
                watch_for_file(frame_path)

            # -----------------------------------------------------
            # Raw data file successfully created
            # -----------------------------------------------------
            if frame_path.is_file():
                num_bytes = (frame_path.stat().st_size)

                with tracer.start_as_current_span("request DSOC storage"):
                    send_kafka_message(
                        producer_topic=producer_topic,
                        producer_config=producer_config,
                        waveform_requester=waveform_requester,
                        message_type=(Message.VLBA_REQUEST_STORAGE),
                        transfer_uuid=transfer_uuid,
                        gbt_uuid=gbt_uuid,
                        gbt_event_time=gbt_event_time,
                        station=STATION,
                        status=Status.QUEUED,
                        object_id=object_id,
                        target=target,
                        tx_waveform=tx_waveform,
                        rec_waveform=rec_waveform,
                        num_bytes=num_bytes,
                        filename=frame_path.name,
                        xmit_station=Stations.GBT,
                        rcvr_station=STATION,
                        message=(
                            "VLBA requested a storage "
                            "check at DSOC."
                        )
                    )

                print(
                    "VLBA requesting DSOC "
                    "check storage..."
                )

            # -----------------------------------------------------
            # Raw data file creation failed
            # -----------------------------------------------------
            else:
                send_kafka_message(
                    producer_topic=producer_topic,
                    producer_config=producer_config,
                    waveform_requester=waveform_requester,
                    message_type=(Message.VLBA_FAILED),
                    transfer_uuid=transfer_uuid,
                    gbt_uuid=gbt_uuid,
                    gbt_event_time=gbt_event_time,
                    status=Status.FAILED,
                    object_id=object_id,
                    target=target,
                    tx_waveform=tx_waveform,
                    rec_waveform=rec_waveform,
                    num_bytes=0,
                    filename=frame_path.name,
                    station=STATION,
                    xmit_station=Stations.GBT,
                    rcvr_station=STATION,
                    message=(
                        "VLBA source file "
                        "does not exist."
                    ),
                )

                print(
                    "Source file does not exist."
                )

                return True

        # =========================================================
        # DSOC -> VLBA
        #
        # DSOC responded to our storage request.
        # =========================================================
        elif (incoming_key == str(Message.DSOC_RESPOND_STORAGE.value)):

            payload = json.loads(msg.value().decode("utf-8"))
            tag_observation(span, payload)

            station = payload["rcvr_station"]

            # Check if the Kafka message is for this station
            if Stations(station) != STATION:
                return

            print(
                "Received DSOC's storage "
                "check response!"
            )

            transfer_uuid = payload["transfer_uuid"]
            gbt_uuid = payload["gbt_uuid"]
            gbt_event_time = payload.get("gbt_event_time")
            object_id = payload.get("object_id")
            target = payload.get("target")
            waveform_requester = payload.get("waveform_requester")
            tx_waveform = payload.get("tx_waveform")
            rec_waveform = payload.get("rec_waveform")
            num_bytes = int(payload.get(
                "num_bytes",
                0,
            )
            )
            filename = payload.get("filename")
            retry_count = int(payload.get(
                "retry_count",
                0,
            )
            )
            response_status = int(payload["status"])

            # =====================================================
            # DSOC storage check permanently failed
            # =====================================================
            if (response_status == Status.FAILED.value):
                print(
                    "DSOC storage request "
                    f"failed: {payload['message']}"
                )

                return True

            # =====================================================
            # DSOC HAS STORAGE
            # =====================================================
            elif (response_status == Status.READY.value):

                attempts = 0

                while True:
                    try:
                        # -----------------------------------------
                        # VLBA is beginning the actual transfer.
                        #
                        # This Kafka message:
                        #
                        # - tells the progress tracking worker that transmission started
                        # - records VLBA TRANSFERRING through
                        #   db_consumer
                        # -----------------------------------------
                        send_kafka_message(
                            producer_topic=("progress_tracking"),
                            producer_config=(producer_config),
                            waveform_requester=waveform_requester,
                            message_type=(Message.VLBA_TRANSFERRING),
                            transfer_uuid=(transfer_uuid),
                            gbt_uuid=gbt_uuid,
                            gbt_event_time=(gbt_event_time),
                            station=STATION,
                            status=(Status.TRANSFERRING),
                            object_id=object_id,
                            target=target,
                            tx_waveform=tx_waveform,
                            rec_waveform=(rec_waveform),
                            num_bytes=num_bytes,
                            filename=filename,
                            xmit_station=Stations.GBT,
                            rcvr_station=STATION,
                            message=(
                                "Hancock VLBA has "
                                "started sending the "
                                "data file to DSOC "
                                "via e-transfer."
                            ),
                        )

                        frame_path = (raw_data_path / f"{transfer_uuid}.bin")

                        print(
                            "DSOC responded "
                            "affirmative to storage "
                            "check. Initiating "
                            "e-transfer..."
                        )

                        with tracer.start_as_current_span("send e-transfer",
                                                          attributes={"ngradar.transfer.attempt": attempts + 1,
                                                                      "ngradar.transfer_uuid": str(transfer_uuid),
                                                                      "ngradar.transfer.total_bytes": num_bytes}):
                            etc_send(frame_path)

                        # -----------------------------------------
                        # etc_send returned successfully.
                        #
                        # VLBA now knows that its side of the
                        # transfer completed successfully.
                        # -----------------------------------------
                        print("E-Transfer completed successfully.")

                        break

                    # =============================================
                    # Known ETC/e-transfer process failure
                    # =============================================
                    except subprocess.CalledProcessError as exc:
                        failure_reason = (
                            FAILURE_REASONS.get(
                                exc.returncode,
                                "The e-transfer failed",
                            )
                        )

                        failure_message = (
                            f"{failure_reason} "
                            "mid-transfer. "
                            "Transfer interrupted. "
                            f"(return code: "
                            f"{exc.returncode})"
                        )

                        print(
                            "E-transfer failed with "
                            "return code: "
                            f"{exc.returncode}"
                        )

                        send_kafka_message(
                            producer_topic=(producer_topic),
                            producer_config=(producer_config),
                            waveform_requester=waveform_requester,
                            message_type=(Message.VLBA_FAILED),
                            transfer_uuid=(transfer_uuid),
                            gbt_uuid=gbt_uuid,
                            gbt_event_time=(gbt_event_time),
                            station=STATION,
                            status=Status.FAILED,
                            object_id=object_id,
                            target=target,
                            tx_waveform=tx_waveform,
                            rec_waveform=(rec_waveform),
                            num_bytes=num_bytes,
                            filename=filename,
                            xmit_station=Stations.GBT,
                            rcvr_station=STATION,
                            message=(failure_message),
                        )

                        attempts += 1

                        if (attempts >= MAX_RESUME_ATTEMPTS):
                            print(
                                "E-transfer failed "
                                f"{attempts} times. "
                                "Giving up."
                            )

                            span.set_status(
                                TraceStatus(StatusCode.ERROR, "e-transfer retries exhausted or daemon unavailable"))
                            return False

                        print(
                            "Waiting for the "
                            "e-transfer daemon to "
                            "come back..."
                        )

                        with tracer.start_as_current_span("wait for e-transfer daemon"):
                            if not wait_for_etd():
                                print(
                                    "E-transfer daemon "
                                    "never came back. "
                                    "Giving up."
                                )

                                span.set_status(
                                    TraceStatus(StatusCode.ERROR, "e-transfer retries exhausted or daemon unavailable"))
                                return False

                        print(
                            "E-transfer daemon is "
                            "back. Resuming the "
                            "transfer..."
                        )

                    # =============================================
                    # Unexpected transfer failure
                    # =============================================
                    except Exception as exc:
                        span.record_exception(exc)
                        span.set_status(TraceStatus(StatusCode.ERROR, str(exc)))
                        print(
                            "Unexpected e-transfer "
                            f"failure: {exc}"
                        )

                        send_kafka_message(
                            producer_topic=(producer_topic),
                            producer_config=(producer_config),
                            waveform_requester=waveform_requester,
                            message_type=(Message.VLBA_FAILED),
                            transfer_uuid=(transfer_uuid),
                            gbt_uuid=gbt_uuid,
                            gbt_event_time=(gbt_event_time),
                            station=STATION,
                            status=Status.FAILED,
                            object_id=object_id,
                            target=target,
                            tx_waveform=tx_waveform,
                            rec_waveform=(rec_waveform),
                            num_bytes=num_bytes,
                            filename=filename,
                            xmit_station=Stations.GBT,
                            rcvr_station=STATION,
                            message=(
                                "The e-transfer "
                                "failed unexpectedly "
                                "mid-transfer. "
                                "Transfer interrupted. "
                                f"({exc})"
                            ),
                        )

                        return False

            # =====================================================
            # DSOC DOES NOT HAVE STORAGE
            # =====================================================
            else:
                print(
                    "DSOC responded negative to "
                    "storage check. Will ask "
                    "again in 5 seconds..."
                )

                with tracer.start_as_current_span("wait before DSOC storage retry"):
                    time.sleep(5)

                # VLBA remains BLOCKED but performs the
                # same workflow action again:
                #
                # VLBA_REQUEST_STORAGE
                #
                # Message type = action
                # Status       = current state
                with tracer.start_as_current_span("request DSOC storage"):
                    send_kafka_message(
                        producer_topic=producer_topic,
                        producer_config=producer_config,
                        waveform_requester=waveform_requester,
                        message_type=(Message.VLBA_REQUEST_STORAGE),
                        retry_count=retry_count,
                        transfer_uuid=transfer_uuid,
                        gbt_uuid=gbt_uuid,
                        gbt_event_time=gbt_event_time,
                        station=STATION,
                        status=Status.BLOCKED,
                        object_id=object_id,
                        target=target,
                        tx_waveform=tx_waveform,
                        rec_waveform=rec_waveform,
                        num_bytes=num_bytes,
                        filename=filename,
                        xmit_station=Stations.GBT,
                        rcvr_station=STATION,
                        message=(
                            "Waiting for DSOC "
                            "storage availability."
                        ),
                    )

        # =========================================================
        # DSOC -> VLBA
        #
        # DSOC says the VLBA raw file can be deleted.
        # =========================================================
        elif (incoming_key == str(Message.VLBA_DELETE.value)):
            payload = json.loads(msg.value().decode("utf-8"))
            tag_observation(span, payload)
            station = payload["rcvr_station"]

            # Check if the Kafka message is for this station
            if Stations(station) != STATION:
                return

            file_name = payload["filename"]

            with tracer.start_as_current_span("delete VLBA raw data"):
                delete_observation_data(file_name)

        # =========================================================
        # Encountered a message for a different VLBA station
        # than whichever VLBA station is currently processing
        # =========================================================
        else:
            print(
                "NOT A VALID KAFKA "
                "MESSAGE VALUE!"
            )

        return True

    # Catch any unexpected exceptions and record them in the span
    except Exception as exc:
        span.record_exception(exc)
        span.set_status(TraceStatus(StatusCode.ERROR, str(exc)))
        raise

    finally:
        detach(token)  # Detach the context to avoid leaking it to other spans
        span.end()


class Command(BaseCommand):
    help = "Runs the VLBA simulator"

    def handle(
            self,
            *args,
            **options,
    ):
        print("Starting VLBA simulator")

        STATION = Stations[os.environ.get("STATION_NAME")]

        provider = TracerProvider(
            sampler=ALWAYS_ON,
            resource=Resource.create({
                "service.name": os.environ.get("OTEL_SERVICE_NAME") or f"vlba-{STATION.name.lower()}",
                "ngradar.vlba.station": STATION.name,
            }),
        )
        processor = BatchSpanProcessor(OTLPSpanExporter(
            endpoint="http://otel-collector:4317"))  # endpoint will not work on droplets - configuring that later
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

        (
            producer_topic,
            producer_config,
            consumer_topic,
            consumer_config,
        ) = bootstrap(
            STATION
        )

        # process_msg can remain blocked while
        # wait_for_etd() waits for the daemon to
        # return.
        #
        # Increase Kafka's allowed poll interval
        # accordingly.
        consumer_config[
            "max.poll.interval.ms"
        ] = (
                    (
                            ETD_MAX_CONN_RETRY
                            * ETD_RETRY_CONN_DELAY
                    )
                    + 300
            ) * 1000

        consume(
            consumer_topic,
            consumer_config,
            process_msg,
            producer_topic=producer_topic,
            producer_config=producer_config,
            manual_commit=True,
        )