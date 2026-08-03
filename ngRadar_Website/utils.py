from datetime import datetime, timezone
# from confluent_kafka.admin import AdminClient, NewTopic, KafkaException, KafkaError
from dotenv import load_dotenv
from ngRadar_Website.enums import Stations
from confluent_kafka import Consumer
import boto3
import os
import time
from botocore.config import Config
from botocore.exceptions import (
    EndpointConnectionError,
    ConnectionError,
    ClientError,
)
import subprocess

#Program constants
SESSION_TIMEOUT_MS = 45000
MAX_BYTES = 8388608

def latency_calc(event_time, sim=None):
    """
    Description: Calculates the latency of the message from the time it was sent to the time it was received
    Inputs: event_time = Time in the past. This is the time when the 'stopwatch' starts on our latency calculation
            sim = the sim file in use (GBT or DSOC)
    Returns: latency_ms = Latency in milliseconds
    """
    current_time = datetime.now(timezone.utc)
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
    if sim in [Stations.GBT, Stations.HN]:
        type = "producer and consumer"
        if sim == Stations.GBT:
            # GBT consumes from UI, produces to GBT
            topic1 = "user_input"
            topic2 = "GBT_data"
        else:
            # VLBA consumes from GBT, produces to VLBA
            topic1 = "GBT_data"
            topic2 = "VLBA_data"
    elif sim == Stations.DSOC:
        # DSOC is now consuming from VLBA
        type = "consumer"
        topic = ["VLBA_data"]  #consumes from the GBT's topic
    else:  # sim == Stations.UI:
        # UI produces to UI topic
        type = "producer"
        topic = "user_input"

    # perform the shared behavior for each type:
    if type == "producer and consumer":
        # # config for both producer and consumer sims
        # print("BOOTSTRAP =", bootstrap)
        # admin = AdminClient({"bootstrap.servers": bootstrap})
        # topics = [
        #     NewTopic(topic1, num_partitions=3, replication_factor=1),
        #     NewTopic(topic2, num_partitions=1, replication_factor=1),
        # ]
        # fs = admin.create_topics(topics, request_timeout=30)

        # for topic, f in fs.items():
        #     # f is a Future; result() will raise if creation failed for reasons other than "already exists"
        #     try:
        #         f.result()
        #         print(f"Created topic {topic}")
        #     # handle the case where it tried to create a topic that already exists:
        #     except KafkaException as e:
        #         if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
        #             print(f"Failed creating topic {topic}: {e!r}")
        #             raise
        
        producer_topic = topic2  # NOTE The topic to which the messages will be sent, rename accordingly to whatever topic you want to send to
        producer_config = {
            "bootstrap.servers": bootstrap,
            "message.max.bytes": MAX_BYTES,# NOTE can make this constant
            "client.id": f"{sim.name.lower()}-producer",
        }

        consumer_topic = [topic1]
        consumer_config = {
            "bootstrap.servers": bootstrap,
            "fetch.max.bytes": MAX_BYTES,
            "session.timeout.ms": SESSION_TIMEOUT_MS,
            "client.id": f"{sim.name.lower()}-consumer",
            "group.id": f"{sim.name.lower()}-consumer-group",
            "auto.offset.reset": "earliest",
        }
        return producer_topic, producer_config, consumer_topic, consumer_config
    elif type == "consumer":
        # config for just consumer
        
        config = {
            "bootstrap.servers": bootstrap,
            "fetch.max.bytes": MAX_BYTES,
            "session.timeout.ms": SESSION_TIMEOUT_MS,
            "client.id": f"{sim.name.lower()}-consumer",
            "group.id": f"{sim.name.lower()}-consumer-group",
            "auto.offset.reset": "earliest",
        }
    else:  # type == "producer"
        # config for just producer

        config = {
            "bootstrap.servers": bootstrap,
            "message.max.bytes": MAX_BYTES,
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


def consume(topic, config, process_msg, producer_topic=None, producer_config=None):
    """
    Description: Creates a new consumer instance; subscribes to a Kafka topic and receives messages.
    Inputs: topic = The Kafka topic to receieve messages from.
            config = Server configuration defining the bootstrap, byte and timeout limits, and IDs.
            process_msg = A function which accepts the Kafka message as an input.
    Returns: N/A
    """
    consumer = Consumer(config)

    #subscribes to the specified topic
    consumer.subscribe(topic)
    
    try:
        while True:
            #consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0) #polls for messages for 1 second
            
            if msg is None:
                continue
            if msg.error() is not None:
                print("Consumer error:", msg.error())
                continue

            #if msg is not None and msg.error() is None:
            process_msg(msg, producer_topic, producer_config)
    except Exception as e:
        import traceback
        print("An unhandled exception occurred in the consumer loop:")
        traceback.print_exc()
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

    for attempt in range(30):
        try:
            s3.list_buckets()
            print("SeaweedFS S3 is ready.")
            break

        except (EndpointConnectionError, ConnectionError):
            print(f"Waiting for SeaweedFS... ({attempt + 1}/30)")
            time.sleep(2)

        except ClientError as e:
            # The S3 API is responding, so we're ready.
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



# etransfer send data from client -> daemon util function
def etc_send(frame_path):
    """
    Sends data from the client to the daemon using e-transfer.
    Daemon is already set up when dsoc etd container starts (etr daemon)

    Input: 
        frame_path = Path to the file that we want to send to the daemon. On the client machine.
    Output: 
        Command line output of the etc command, which will show the progress of the transfer and any errors that may occur. Overwrite flag used for now to demmonstrate sequencing even if same file is being sent multiple times.
        Use --resume flag in production.
    """

    subprocess.run(
        [
            "etc",
            str(frame_path),
            os.environ["ETD_DESTINATION"], # env variable set in docker compose - will need to change in production to point to the actual daemon destination that is not localhost
            "--overwrite",
        ],
        check=True,
    )