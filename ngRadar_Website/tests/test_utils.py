from datetime import datetime, timedelta, timezone
import os
import pytest
from unittest.mock import patch, MagicMock
from ngRadar_Website.enums import Stations
from botocore.config import Config
from botocore.exceptions import (
    EndpointConnectionError,
    ConnectionError,
    ClientError,
)


# ===============================================
# Here we can test all of our utility functions
# ===============================================

# ==============================================================================
# IMPORTANT:
# Because we read "ngrok_endpoint.env" on import, we need to patch the Path globally
# before importing all the functions we want to test.
# ==============================================================================
mock_env_data = "BOOTSTRAP_SERVER=localhost:9092\nSOME_OTHER_VAR=value" 
with patch("pathlib.Path.read_text", return_value=mock_env_data):
    from ngRadar_Website.utils import (
        latency_calc,
        config_func,
        bootstrap,
        consume,
        create_s3_client,
        ensure_bucket_exists,
        etc_send,
        watch_for_file,
    )

# ==============================================================================
# 1. latency_calc
# ==============================================================================

#can add as many different latency test values here as you want:
@pytest.mark.parametrize("event_time, expected", [
        (datetime.now(timezone.utc) - timedelta(seconds=1), 1000),
        (datetime.now(timezone.utc) - timedelta(seconds=2), 2000)
    ])
def test_latency_calc_dsoc(event_time, expected):
    """Scenario 1: sim is DSOC"""
    sim = Stations.DSOC
    latency = latency_calc(event_time, sim)

    upper_bound = expected+300
    
    # 3. Assert (1 second = 1000 milliseconds)
    assert expected <= latency < upper_bound, f"Expected latency around 1000 ms, got {latency} ms"


@pytest.mark.parametrize("event_time, expected", [
        (datetime.now(timezone.utc) - timedelta(seconds=1), 1000),
        (datetime.now(timezone.utc) - timedelta(seconds=2), 2000)
    ])
def test_latency_calc_none(event_time, expected):
    """Scenario 2: sim is not provided"""
    latency = latency_calc(event_time)

    upper_bound = expected+300
    
    # 3. Assert (1 second = 1000 milliseconds)
    assert expected <= latency < upper_bound, f"Expected latency around 1000 ms, got {latency} ms"


@pytest.mark.parametrize("event_time, expected", [
        (datetime.now(timezone.utc) - timedelta(seconds=1), -4000),
        (datetime.now(timezone.utc) - timedelta(seconds=2), -3000),
        (-1, 0)
    ])
def test_latency_calc_gbt(event_time, expected):
    """Scenario 3: sim is gbt"""
    sim = Stations.GBT
    latency = latency_calc(event_time, sim)

    upper_bound = expected+300
    
    # 3. Assert (1 second = 1000 milliseconds)
    assert expected <= latency < upper_bound, f"Expected latency around {expected} ms, got {latency} ms"


# ==============================================================================
# 2. config_func
# ==============================================================================

# NOTE I attempted to make these two scenarios into one test with parametrize, but because they have a different number of variables/outputs, it was too awkward

def test_config_func_GBT():
    """Scenario 1: sim is GBT"""
    sim = Stations.GBT
    bootstrap = "12345"

    producer_topic, producer_config, consumer_topic, consumer_config = config_func(sim, bootstrap)

    assert producer_topic == "GBT_data"
    assert producer_config == {
            "bootstrap.servers": bootstrap,
            "message.max.bytes": 8388608,
            "client.id": "gbt-producer"
        }
    assert consumer_topic == ["user_input"]
    assert consumer_config == {
            "bootstrap.servers": bootstrap,
            "fetch.max.bytes": 8388608,
            "session.timeout.ms": 45000,
            "client.id": "gbt-consumer",
            "group.id": "gbt-consumer-group",
            "auto.offset.reset": "earliest",
        }


# @pytest.mark.parametrize("sim", [
#         (Stations.SC),
#         (Stations.HN),
#         (Stations.FD)
#     ])
# NOTE: I want to make the code dynamically accept all VLBA stations, but that is a future project
def test_config_func_vlba():
    """Scenario 2: sim is a VLBA site"""
    sim = Stations.HN
    bootstrap = "12345"

    producer_topic, producer_config, consumer_topic, consumer_config = config_func(sim, bootstrap)

    assert producer_topic == "VLBA_data"
    assert producer_config == {
            "bootstrap.servers": bootstrap,
            "message.max.bytes": 8388608,
            "client.id": f"{sim.name.lower()}-producer"
        }
    assert consumer_topic == ["GBT_data"]
    assert consumer_config == {
            "bootstrap.servers": bootstrap,
            "fetch.max.bytes": 8388608,
            "session.timeout.ms": 45000,
            "client.id": f"{sim.name.lower()}-consumer",
            "group.id": f"{sim.name.lower()}-consumer-group",
            "auto.offset.reset": "earliest",
        }


def test_config_func_DSOC():
    """Scenario 3: sim is DSOC"""

    sim = Stations.DSOC
    bootstrap = "12345"

    topic, config = config_func(sim, bootstrap)

    assert topic == ["VLBA_data"]
    assert config == {
            "bootstrap.servers": bootstrap,
            "fetch.max.bytes": 8388608,
            "session.timeout.ms": 45000,
            "client.id": "dsoc-consumer",
            "group.id": "dsoc-consumer-group",
            "auto.offset.reset": "earliest",
        }

# ==============================================================================
# 3. bootstrap Test
# ==============================================================================

@patch("ngRadar_Website.utils.load_dotenv")
@patch("ngRadar_Website.utils.config_func")
@patch("ngRadar_Website.utils.os.getenv")
def test_bootstrap_GBT(mock_os_getenv, mock_config_func, mock_load_dotenv):
    """Scenario 1: sim = GBT"""
    sim = Stations.GBT

    mock_env_data = "BOOTSTRAP_SERVER=fake_bootstrap\nSOME_OTHER_VAR=value"
    mock_load_dotenv.return_value = mock_env_data
    mock_os_getenv.return_value = "fake_bootstrap"

    mock_config_func.return_value = (
        "GBT_data",
        {"bootstrap.servers": "fake_bootstrap"},
        ["user_input"],
        {"bootstrap.servers": "fake_bootstrap"},
    )

    producer_topic, producer_config, consumer_topic, consumer_config = bootstrap(sim)

    mock_config_func.assert_called_once_with(sim, "fake_bootstrap")
    assert producer_topic == "GBT_data"
    assert producer_config == {"bootstrap.servers": "fake_bootstrap"}
    assert consumer_topic == ["user_input"]
    assert consumer_config == {"bootstrap.servers": "fake_bootstrap"}


@patch("ngRadar_Website.utils.load_dotenv")
@patch("ngRadar_Website.utils.config_func")
@patch("ngRadar_Website.utils.os.getenv")
def test_bootstrap_DSOC(mock_os_getenv, mock_config_func, mock_load_dotenv):
    """Scenario 2: sim = DSOC"""
    sim = Stations.DSOC

    mock_env_data = "BOOTSTRAP_SERVER=fake_bootstrap\nSOME_OTHER_VAR=value"
    mock_load_dotenv.return_value = mock_env_data
    mock_os_getenv.return_value = "fake_bootstrap"
    
    mock_config_func.return_value = (
        "VLBA_data",
        {"bootstrap.servers": "fake_bootstrap"},
    )

    topic, config = bootstrap(sim)

    mock_config_func.assert_called_once_with(sim, "fake_bootstrap")
    assert topic == "VLBA_data"
    assert config == {"bootstrap.servers": "fake_bootstrap"}


@patch("ngRadar_Website.utils.load_dotenv")
@patch("ngRadar_Website.utils.config_func")
@patch("ngRadar_Website.utils.os.getenv")
def test_bootstrap_none(mock_os_getenv, mock_config_func, mock_load_dotenv):
    """Scenario 3: sim is UI user input"""
    sim = Stations.UI

    mock_env_data = "BOOTSTRAP_SERVER=fake_bootstrap\nSOME_OTHER_VAR=value"
    mock_load_dotenv.return_value = mock_env_data
    mock_os_getenv.return_value = "fake_bootstrap"

    mock_config_func.return_value = (
        "user_input",
        {"bootstrap.servers": "fake_bootstrap"},
    )

    topic, config = bootstrap(sim)

    mock_config_func.assert_called_once_with(sim, "fake_bootstrap")
    assert topic == "user_input"
    assert config == {"bootstrap.servers": "fake_bootstrap"}



# ==============================================================================
# 4. consume Test
# ==============================================================================

@patch("ngRadar_Website.utils.Consumer")
def test_consume(mock_Consumer):
    """Scenario 1: msg is Not None and error is None"""

    # mock the results of the Consumer and subscribe calls:
    mock_consumer = mock_Consumer.return_value
    mock_consumer.subscribe.return_value = None

    mock_process_msg = MagicMock()

    # make a fake message returned from polling:
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    # to deal with the While loop, stop after one message is polled:
    mock_consumer.poll.side_effect = [
        mock_msg,
        RuntimeError("Stop Test"),
    ]

    #call the function to use our fake values:
    with pytest.raises(RuntimeError):
        consume("topic", "config", mock_process_msg)

    mock_Consumer.assert_called_once_with("config")
    mock_consumer.subscribe.assert_called_once_with("topic")
    mock_process_msg.assert_called_once_with(mock_msg, None, None)


# ==============================================================================
# 4. create_s3_client Test
# ==============================================================================

@patch.dict(
    "os.environ",
    {
        "WEED_S3_ENDPOINT": "fake_endpoint",
        "WEED_S3_ACCESS_KEY": "fake_key",
        "WEED_S3_SECRET_KEY": "fake_secret"
    },
)
@patch("ngRadar_Website.utils.boto3.client")
@patch("ngRadar_Website.utils.ensure_bucket_exists")
@patch("ngRadar_Website.utils.Config")
def test_create_s3_client_success(mock_Config, mock_ensure_bucket, mock_boto3):
    """Scenario 1: s3 client is ready"""
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    mock_s3.list_buckets.return_value = {"Buckets": [{"Name": "fake_bucket"}]}

    mock_ensure_bucket.return_value = None

    config_value = "fake_config"
    mock_Config.return_value = config_value

    s3_client = create_s3_client()

    assert s3_client == mock_s3
    mock_boto3.assert_called_once_with(
        "s3",
        endpoint_url="fake_endpoint",
        aws_access_key_id="fake_key",
        aws_secret_access_key="fake_secret",
        region_name="us-east-1",
                config=config_value
    )
    mock_ensure_bucket.assert_called_once_with(mock_s3)


@patch.dict(
    "os.environ",
    {
        "WEED_S3_ENDPOINT": "fake_endpoint",
        "WEED_S3_ACCESS_KEY": "fake_key",
        "WEED_S3_SECRET_KEY": "fake_secret"
    },
)
@patch("ngRadar_Website.utils.boto3.client")
@patch("ngRadar_Website.utils.time.sleep")
@patch("ngRadar_Website.utils.ensure_bucket_exists")
@patch("ngRadar_Website.utils.Config")
def test_create_s3_client_connection_error(mock_Config, mock_ensure_bucket, mock_sleep, mock_boto3):
    """Scenario 2: connection error"""
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    mock_s3.list_buckets.side_effect = EndpointConnectionError(endpoint_url=os.environ["WEED_S3_ENDPOINT"])

    mock_ensure_bucket.return_value = None

    config_value = "fake_config"
    mock_Config.return_value = config_value

    mock_sleep.return_value = None

    with pytest.raises(RuntimeError) as exc_info:
        s3_client = create_s3_client()

    assert mock_sleep.call_count == 30
    mock_boto3.assert_called_once_with(
        "s3",
        endpoint_url="fake_endpoint",
        aws_access_key_id="fake_key",
        aws_secret_access_key="fake_secret",
        region_name="us-east-1",
                config=config_value
    )
    mock_ensure_bucket.assert_not_called()
    assert mock_s3.list_buckets.call_count == 30

#NOTE: needs one more scenario for client error


# ==============================================================================
# 5. ensure_bucket_exists Test
# ==============================================================================

@patch.dict(
    "os.environ",
    {
        "WEED_S3_BUCKET": "fake_bucket",
    },
)
def test_ensure_bucket_exists():
    """Scenario 1: bucket exists"""
    mock_s3 = MagicMock()
    mock_s3.head_bucket.return_value = {"Buckets": [{"Name": "fake_bucket"}]}

    ensure_bucket_exists(mock_s3)

    mock_s3.head_bucket.assert_called_once_with(Bucket="fake_bucket")
    mock_s3.create_bucket.assert_not_called()


@patch.dict(
    "os.environ",
    {
        "WEED_S3_BUCKET": "fake_bucket",
    },
)
def test_ensure_bucket_exists_created():
    """Scenario 2: bucket needs to be created"""
    mock_s3 = MagicMock()
    mock_s3.head_bucket.side_effect = ClientError(
        error_response={
            "Error": {
                "Code": "404",
                "Message": "Not Found",
            },
            "ResponseMetadata": {
                "HTTPStatusCode": 404,
            },
        },
        operation_name="HeadBucket",
    )
    mock_s3.create_bucket.return_value = None

    ensure_bucket_exists(mock_s3)

    mock_s3.head_bucket.assert_called_once_with(Bucket="fake_bucket")
    mock_s3.create_bucket.assert_called_once_with(Bucket="fake_bucket")


# ==============================================================================
# 5. etc_send Test
# ==============================================================================

@patch.dict(
    "os.environ",
    {
        "ETD_DESTINATION": "fake_path",
    },
)
@patch("ngRadar_Website.utils.uuid.uuid4")
@patch("ngRadar_Website.utils.os.openpty")
@patch("ngRadar_Website.utils.subprocess.Popen")
@patch("ngRadar_Website.utils.os.close")
@patch("ngRadar_Website.utils.select.select")
@patch("ngRadar_Website.utils.os.read")
@patch("ngRadar_Website.utils.parse_etc_progress")
def test_etc_send(mock_parse, mock_os_read, mock_select, mock_os_close, mock_popen, mock_os_open, mock_uuid):
    mock_frame_path = MagicMock()
    mock_frame_path.stat.return_value.st_size = 500

    mock_uuid.return_value = "fake_uuid"

    mock_master = "fake_master_fd"
    mock_slave = "fake_slave_fd"
    mock_os_open.return_value = (mock_master, mock_slave)

    mock_process = MagicMock()
    mock_popen.return_value = mock_process

    mock_os_close.return_value = None

    mock_select.return_value = ([mock_master], [], [])

    mock_os_read.return_value = b"50% 250/500\r"

    mock_parse.return_value = None

    #only run through the while loop twice to avoid infinite loop in test:
    mock_process.poll.side_effect = [None, 0]
    mock_process.wait.return_value = 0
    mock_process.args = ["etc", "fake_file"]

    etc_send(mock_frame_path)

    mock_uuid.assert_called_once()
    mock_os_open.assert_called_once()
    mock_popen.assert_called_once_with(
        ["etc", str(mock_frame_path), os.environ["ETD_DESTINATION"], "--overwrite",],
        stdin=mock_slave,
        stdout=mock_slave,
        stderr=mock_slave,
        close_fds=True,
    )
    assert mock_os_close.call_count == 2
    mock_os_read.assert_called_once_with(mock_master, 4096)
    mock_parse.assert_called_once_with("50% 250/500", expected_num_bytes=500, transfer_id=mock_uuid.return_value)


# ==============================================================================
# 5. watch_for_file Test
# ==============================================================================

@patch("ngRadar_Website.utils.subprocess.run")
@patch("ngRadar_Website.utils.time.sleep")
def test_watch_for_file(mock_sleep, mock_subprocess):
    file_path = "filepath"

    first_result = MagicMock()
    first_result.stdout = "exists"
    second_result = MagicMock()
    second_result.stdout = ""

    mock_subprocess.side_effect = [first_result, second_result]

    mock_sleep.return_value = None

    watch_for_file(file_path)

    first = mock_subprocess.call_args_list[0]
    second = mock_subprocess.call_args_list[1]

    mock_sleep.assert_called_once()
    assert mock_subprocess.call_count == 2
    assert first.kwargs["capture_output"] == True
    assert second.kwargs["capture_output"] == True