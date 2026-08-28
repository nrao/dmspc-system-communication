from datetime import datetime, timezone
import uuid
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv
from ngRadar_Website.enums import Stations, Status
from ngRadar_Website.models.models import gbtEvent, dsocEvent, ETransferEvent, ObservatoryEvent
from confluent_kafka import Consumer, Producer, KafkaError
import boto3
import os
import time
import subprocess
import random
import re
import json
import select

from botocore.config import Config
from botocore.exceptions import (
    EndpointConnectionError,
    ConnectionError,
    ClientError,
)
from pathlib import Path
from confluent_kafka import Producer

# regex patterns to match the progress output of the etc command
PROGRESS_RE = re.compile(
    r"\]\s+"
    r"(?P<percent>\d+(?:\.\d+)?)%\s+"
    r"(?P<received>\d+(?:\.\d+)?)\s+(?P<received_unit>\S+)\s+/\s+"
    r"(?P<total>\d+(?:\.\d+)?)\s+(?P<total_unit>\S+)"
)
ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

#Program constants
SESSION_TIMEOUT_MS = 10000
MAX_BYTES = 8388608


def latency_calc(event_time, sim=None, current_time=None):
    """
    Description: Calculates the latency of the message from the time it was sent to the time it was received
    Inputs: event_time = Time in the past. This is the time when the 'stopwatch' starts on our latency calculation
            sim = the sim file in use (GBT or DSOC)
    Returns: latency_ms = Latency in milliseconds
    """
    if current_time is None:
        current_time = datetime.now(timezone.utc)
    else:
        current_time = current_time

    if sim == Stations.GBT:
        if event_time == -1:
                latency_ms = 0 #NOTE We are currently setting latency = 0 for the very first gbt payload, which is not triggered by a UI event. I want to make this a Null field in the future (will require a migration)
        else:
            latency = current_time - event_time
            latency_ms = latency.total_seconds() * 1000 - 5000 #accounting for time.sleep
    else:
        latency = current_time - event_time
        latency_ms = latency.total_seconds() * 1000
    return latency_ms


def config_func(sim, bootstrap):
    """
    Description: Generates config file and Kafka topic info, based on the sim.
                Designed to be called in conjunction with bootstrap function.
    Inputs: sim = the sim file in use (GBT, DSOC, or VLBA)
            bootstrap = bootstrap info derived from .env
    Returns: topic(s) and config(s) variables
    """

    # determine the type of sim being used - each one has unique kafka topics:
    if sim in [Stations.GBT, Stations.HN, Stations.DSOC]:
        type = "producer and consumer"
        if sim == Stations.GBT:
            # GBT consumes from UI, produces to GBT
            topic1 = ["user_input"]
            topic2 = "GBT_data"
        elif sim == Stations.HN:
            # VLBA consumes from GBT and DSOC, produces to DSOC
            topic1 = ["GBT_data", "DSOC_notif"]
            topic2 = "VLBA_notif"
        elif sim == Stations.DSOC:
            # DSOC is now consuming from and producing to VLBA
            topic1 = ["VLBA_notif"]  #consumes from the GBT's topic
            topic2 = "DSOC_notif"
    else:  # sim == Stations.UI:
        # UI produces to UI topic
        type = "producer"
        topic = "user_input"

    # perform the shared behavior for each type:
    if type == "producer and consumer":

        producer_topic = topic2  # NOTE The topic to which the messages will be sent, rename accordingly to whatever topic you want to send to
        producer_config = {
            "bootstrap.servers": bootstrap,
            "message.max.bytes": MAX_BYTES,# NOTE can make this constant
            "message.timeout.ms": 2000,
            "client.id": f"{sim.name.lower()}-producer",
        }

        consumer_topic = topic1
        consumer_config = {
            "bootstrap.servers": bootstrap,
            "fetch.max.bytes": MAX_BYTES,
            "session.timeout.ms": SESSION_TIMEOUT_MS,
            "client.id": f"{sim.name.lower()}-consumer",
            "group.id": f"{sim.name.lower()}-consumer-group",
            "auto.offset.reset": "earliest",
        }  # TODO make sure this works
        return producer_topic, producer_config, consumer_topic, consumer_config
    # elif type == "consumer": #NOTE Not being used right now. Commented out to help testcov
    #     # config for just consumer
        
    #     config = {
    #         "bootstrap.servers": bootstrap,
    #         "fetch.max.bytes": MAX_BYTES,
    #         "session.timeout.ms": SESSION_TIMEOUT_MS,
    #         "client.id": f"{sim.name.lower()}-consumer",
    #         "group.id": f"{sim.name.lower()}-consumer-group",
    #         "auto.offset.reset": "earliest",
    #     }
    else:  # type == "producer"
        # config for just producer
        config = {
            "bootstrap.servers": bootstrap,
            "message.max.bytes": MAX_BYTES,
            "message.timeout.ms": 2000,
            "client.id": f"{sim.name.lower()}-producer",
        }

    return topic, config


def bootstrap(sim):
    """
    Description: Extracts bootstrap info from .env and ngrok, then uses config_func to generate outputs
    Inputs: sim = the sim file in use (GBT, DSOC, or VLBA)
    Returns: topic(s) and config(s) variables
    """
    load_dotenv()  # Load environment variables from .env file

    # p = Path("../../../../out/ngrok_endpoint.env")
    # text = p.read_text().strip()

    # bootstrap = None
    # for line in text.splitlines():
    #     if line.startswith("BOOTSTRAP_SERVER="):
    #         bootstrap = line.split("=", 1)[1].strip()
    #         break

    # if not bootstrap:
    #     raise RuntimeError("BOOTSTRAP_SERVER not found in /out/ngrok_endpoint.env")

    bootstrap = os.getenv("BOOTSTRAP_SERVER", "kafka-broker:29092")
    
    # if sim != Stations.DSOC:
    #     producer_topic, producer_config, consumer_topic, consumer_config = config_func(sim, bootstrap)
    #     return producer_topic, producer_config, consumer_topic, consumer_config
    # else:
    #     topic, config = config_func(sim, bootstrap)
    #     return topic, config

    return config_func(sim, bootstrap)
    

# def views_bootstrap():
#     from dotenv import load_dotenv
#     load_dotenv(override=True)


def consume(topic, config, process_msg, producer_topic=None, producer_config=None, manual_commit=False):
    """
    Description: Creates a new consumer instance; subscribes to a Kafka topic and receives messages.
    Inputs: topic = The Kafka topic to receieve messages from.
            config = Server configuration defining the bootstrap, byte and timeout limits, and IDs.
            process_msg = A function which accepts the Kafka message as an input.
            manual_commit = If True, a message is only marked done once process_msg returns True,
                so work killed mid e-transfer is redelivered on restart and etc --resume picks up
                the partial file. Callers that opt in MUST have process_msg return True/False.
    Returns: N/A
    """
    try:
        if manual_commit:
            # Copy rather than mutate: bootstrap() hands the same config dict to other callers.
            config = {**config, "enable.auto.commit": False}

        consumer = Consumer(config)

        #subscribes to the specified topic
        consumer.subscribe(topic)
        # TODO make sure works with multiple topics
    
        while True:
            #consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0) #polls for messages for 1 second
            
            if msg is None:
                continue
            if msg.error():
                error = msg.error()

                if error.code() == KafkaError._PARTITION_EOF:
                    print("Consumer reached partition EOF")
                    continue

                print("Consumer error:", error)

                publish_status_obsEvents(
                    status=Status.FAILED,
                    msg="Failed to connect to Kafka.",
                )

                break

            #if msg is not None and msg.error() is None:
            succeeded = process_msg(msg, producer_topic, producer_config)

            if manual_commit and succeeded:
                consumer.commit(msg)
    except Exception as e:
        publish_status_obsEvents(
            status=Status.FAILED,
            msg="Failed to connect to Kafka!",
        )
        raise


def create_s3_client():
    """
    Creates the boto3 S3 client and waits for the S3 gateway
    to become available.
    """
    print("Connecting to:", os.environ["WEED_S3_ENDPOINT"])

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["WEED_S3_ENDPOINT"],
        aws_access_key_id=os.environ["WEED_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["WEED_S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )
    # Change to range(5) if we want enough time to turn seaweed back on during polling
    for attempt in range(3):
        try:
            s3.list_buckets()
            print("SeaweedFS S3 is ready.")
            break

        except (EndpointConnectionError, ConnectionError):
            publish_status_obsEvents(status=Status.POLLING, msg=f"Waiting for SeaweedFS... ({attempt + 1}/3)")
            print(f"Waiting for SeaweedFS... ({attempt + 1}/3)")
            time.sleep(1)

        except ClientError as e:
            print(f"SeaweedFS responded: {e.response['Error']['Code']}")
            break
    else:
        raise RuntimeError("SeaweedFS S3 never became ready")

    ensure_bucket_exists(s3)
    return s3


# def get_presigned_url(s3, event):
#     """
#     Gets a presigned url, specifically for the serve_image in the views,
#     But could be used elsewhere if we grow our website to rendering more 
#     pages with images.
#     """

#     presigned_url = s3.generate_presigned_url(
#             'get_object',
#             Params={
#                 'Bucket': os.environ["WEED_S3_BUCKET"], 
#                 'Key': event.image_key
#             },
#             ExpiresIn=3600 # The link is valid for 1 hour (3600 seconds)
#         )
#     print("Generated:", presigned_url)

#     endpoint = os.environ["WEED_S3_ENDPOINT"]
#     public = os.environ["WEED_S3_PUBLIC_URL"]

#     if endpoint != public:      # will replace for local dev only, in a demo these will be identical (ngrok)
#         presigned_url = presigned_url.replace(endpoint, public)
#         print("After replace:", presigned_url)

#     return presigned_url


def ensure_bucket_exists(s3):
    bucket = os.environ["WEED_S3_BUCKET"]

    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' exists.")
        return

    except ClientError as e:
        code = e.response["ResponseMetadata"]["HTTPStatusCode"]
        print(e.response)

        if code != 404:
            raise

    print(f"Creating bucket '{bucket}'...")
    s3.create_bucket(Bucket=bucket)
    print("Bucket created.")


def upload_seaweedfs(s3, image_key, file_data):
    bucket = os.environ["WEED_S3_BUCKET"]

    s3.put_object(
        Bucket=bucket,
        Key=image_key,
        Body=file_data,
        ContentType="image/png",
    )

    print(f"Success: {image_key}")
    return image_key


#==========================
# etransfer util functions
#=========================

#JSON-writing helper:
def write_transfer_progress(
    *,
    received_bytes,
    total_bytes,
    percent,
    transfer_id,
):
    progress_path = "/service/mock_assets/progress.json"
    temp_path = progress_path + ".tmp"

    progress_data = {
        "received_bytes": received_bytes,
        "total_bytes": total_bytes,
        "percent": percent,
        "transfer_id": transfer_id,
    }

    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(progress_data, f)

    os.replace(temp_path, progress_path)


# Intercepts etc CLI and parses output:
def parse_etc_progress(line, *, expected_num_bytes, transfer_id):
    # Remove terminal escape sequences such as ESC[K.
    clean_line = ANSI_RE.sub("", line)

    match = PROGRESS_RE.search(clean_line)

    if not match:
        return

    percent = float(match.group("percent"))

    # Since we already know the exact source file size,
    # derive received bytes from the percentage instead
    # of parsing etc's human-readable iB/MiB/GiB values.
    received_bytes = round(
        expected_num_bytes * (percent / 100.0)
    )

    if percent >= 100.0:
        received_bytes = expected_num_bytes

    print(
        f"Transfer progress: "
        f"{received_bytes}/{expected_num_bytes} bytes "
        f"({percent:.1f}%)"
    )

    # write_transfer_progress(
    #     received_bytes=received_bytes,
    #     total_bytes=expected_num_bytes,
    #     percent=percent,
    #     transfer_id=transfer_id,
    # )


ETD_MAX_CONN_RETRY = 90     # 90 retries x 10s = waits up to 15 minutes
ETD_RETRY_CONN_DELAY = 10


def consumer_group_has_members(group_id):
    """
    Asks the Kafka broker whether anyone is still a member of group_id.

    The broker drops a consumer that stops heartbeating after SESSION_TIMEOUT_MS,
    so it is the only component that knows whether a sim is alive. A slow transfer
    and a dead sim look identical from the outside, but not to the broker.

    Inputs: group_id = the consumer group to look up, e.g. "hn-consumer-group"
    Output: True if at least one member is in the group, False otherwise.
    """
    admin = AdminClient({"bootstrap.servers": os.environ["BOOTSTRAP_SERVER"]})
    group = admin.describe_consumer_groups([group_id])[group_id].result()
    return len(group.members) > 0


def wait_for_etd():
    """
    Blocks until the e-transfer daemon at ETD_DESTINATION answers again.

    etc --list is the detector: it exits 0 when the daemon replies, so we never
    need etd's port number. etc's own retry flags do the waiting.

    Output: True if the daemon came back, False if it never did.
    """
    result = subprocess.run(
        [
            "etc",
            "--list",
            os.environ["ETD_DESTINATION"],
            "--max-conn-retry", str(ETD_MAX_CONN_RETRY),
            "--retry-conn-delay", str(ETD_RETRY_CONN_DELAY),
        ],
        capture_output=True,
    )
    return result.returncode == 0


# etransfer command to send data from client -> daemon
def etc_send(frame_path):
    """
    Sends data from the client to the daemon using e-transfer.
    Daemon is already set up when dsoc etd container starts (etr daemon)
    Intercepts the output of the etc command and parses live the etc progress and updates progress.json

    Input: 
        frame_path = Path to the file that we want to send to the daemon. On the client machine.
    Output: 
        Command line output of the etc command, showing transfer progress and any errors.
        Uses --resume. Each Kafka message gets a brand-new frame_path, so on a fresh transfer
        --resume behaves like --overwrite; after an interruption it sends only the missing bytes.
    """

    expected_num_bytes = frame_path.stat().st_size
    transfer_id = str(uuid.uuid4())
    # Reset progress at the beginning of a new transfer.
    # write_transfer_progress(
    #     received_bytes=0,
    #     total_bytes=expected_num_bytes,
    #     percent=0.0,
    #     transfer_id=transfer_id,
    # )

    master_fd, slave_fd = os.openpty()

    process = subprocess.Popen(
        [
            "etc",
            str(frame_path),
            os.environ["ETD_DESTINATION"],
            "--resume",
        ],
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        close_fds=True,
    )

    os.close(slave_fd)

    buffer = ""

    try:
        while process.poll() is None:
            readable, _, _ = select.select(
                [master_fd],
                [],
                [],
                0.5,
            )

            if not readable:
                continue

            try:

                terminal_output = os.read(master_fd, 4096).decode(
                    "utf-8",
                    errors="replace",
                )
            except OSError:
                break

            # Print the actual etc output to Docker logs.
            print(terminal_output, end="", flush=True)

            buffer += terminal_output

            # etc redraws the same terminal line using carriage returns.
            parts = re.split(r"[\r\n]", buffer)

            # Save any incomplete piece for the next chunk.
            buffer = parts.pop()

            for line in parts:
                parse_etc_progress(
                    line,
                    expected_num_bytes=expected_num_bytes,
                    transfer_id=transfer_id,
                )

        # Process anything left in the buffer.
        if buffer:
            parse_etc_progress(
                buffer,
                expected_num_bytes=expected_num_bytes,
                transfer_id=transfer_id,
            )

    finally:
        os.close(master_fd)

    return_code = process.wait()

    if return_code != 0:
        raise subprocess.CalledProcessError(
            return_code,
            process.args,
        )


def produce(topic, config, key, value):
    delivery_error = None

    def delivery_report(err, msg):
        nonlocal delivery_error

        if err is not None:
            delivery_error = err

    try:
        # creates a new producer instance
        producer = Producer(config)

        # producing a message to the specified topic 
        producer.produce(topic, key=key, value=value, callback=delivery_report)

        # Give Kafka a limited amount of time to deliver the message
        remaining = producer.flush(2)

        if delivery_error is not None:
            publish_status_obsEvents(
                status=Status.FAILED,
                msg=f"{delivery_error}",
            )
            return False

        if remaining > 0:
            publish_status_obsEvents(
                status=Status.FAILED,
                msg="Kafka broker did not respond.",
            )
            return False

        print(f"Produced message to topic {topic} with key {key}.")
        return True

    except Exception as e:
        publish_status_obsEvents(
            status=Status.FAILED,
            msg=f"Failed to send Kafka message: {e}",
        )
        return False

    
def send_kafka_message(
    *,
    key,
    producer_topic,
    producer_config,
    transfer_uuid,
    gbt_uuid,
    status,
    num_bytes,
    filename,
    message="",
    stations=Stations.HN,
):
    payload = {
        "transfer_uuid": str(transfer_uuid),
        "gbt_uuid": str(gbt_uuid),
        "status": int(status),
        "num_bytes": num_bytes,
        "filename": filename,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "message": message,
        "stations": stations.label,
    }

    produce(
        producer_topic,
        producer_config,
        key,
        json.dumps(payload),
    )

    
def create_file(file_path, file_mb=200):
    file_size_bytes = file_mb * 1024 * 1024
    num_buffers = 100

    buffer_size = file_size_bytes // num_buffers
    remainder = file_size_bytes % num_buffers

    with open(file_path, "wb") as file:
        for i in range(num_buffers):
            size = buffer_size + (1 if i < remainder else 0)
            buffer = random.randbytes(size)
            file.write(buffer)

    print(f"Successfully created a {file_mb}MB random binary file at {file_path}")


def watch_for_file(file_path):
    while True:
        result = subprocess.run(["lsof", file_path], capture_output=True, text=True)
        output = result.stdout
        if output.strip():
            print("Output:\n", output)
        else:
            break

        time.sleep(1)

    # TODO SET ETRANSFER TO READY AND GIVE IT THIS FILE PATH

    
def delete_observation_data(file_name, dir="/raw_data"):
    file_path = Path(dir) / file_name
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Successfully deleted {file_name}")
    else:
        print(f"File {file_name} does not exists")


def get_folder_size(folder_path: Path):
    if not folder_path.exists():
        raise FileNotFoundError(folder_path)

    total = sum(p.stat().st_size for p in folder_path.rglob("*") if p.is_file())
    #print(f"Size of folder: {total} bytes")
    return total

  
# Helper function to record the status of the e-transfer in the ETransferEvent table
def record_transfer_event(
    *,
    transfer_uuid,
    gbt_uuid,
    station,
    status,
    num_bytes=0,
    latency_ms=0.0,
    message="",
):
    gbt_event = gbtEvent.objects.get(uuid=gbt_uuid)

    return ETransferEvent.objects.create(
        transfer_uuid=transfer_uuid,
        gbt_uuid=gbt_uuid,
        object_id=gbt_event.object_id,
        target=gbt_event.target,
        station=station,
        event_time=datetime.now(timezone.utc),
        latency_ms=latency_ms,
        num_bytes=num_bytes,
        status=status,
        message=message,
    )

def publish_status_obsEvents(status, msg):
    """
    Function to be used by all sims to publish failure status and message to the ObservatoryEvent database table.
    """

    data = {
        "object_id": 30104,
        "target": "Moretus",
        "rcvr_station": Stations.HN,
        "xmit_station": Stations.GBT,
        "event_time": datetime.now(timezone.utc),
        "latency_ms": 0.00,
        "status": status,
        "message": msg,
    }

    try:
        # Create and capture the instantiated record model
        record = ObservatoryEvent.objects.create(**data)
        print("Status saved to database successfully.")
    
    except Exception as e:
        print(f"Database error: {e}")