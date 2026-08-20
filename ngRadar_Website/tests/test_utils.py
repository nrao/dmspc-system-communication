from datetime import datetime, timedelta, timezone
import os
import pytest
from unittest.mock import patch, MagicMock
from ngRadar_Website.enums import Stations, Status
from pathlib import Path
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
        create_file,
        delete_observation_data,
        create_s3_client,
        ensure_bucket_exists,
        etc_send,
        watch_for_file,
        produce,
        record_transfer_event,
        send_kafka_message,
        get_folder_size,
        write_transfer_progress,

    )

# ==============================================================================
# 1. latency_calc
# ==============================================================================

#can add as many different latency test values here as you want:
@pytest.mark.parametrize("seconds, expected", [
    (1, 1000),
    (2, 2000)
])
def test_latency_calc_dsoc(seconds, expected):
    sim = Stations.DSOC

    end_time = datetime(2026, 8, 12, 12, 0, 10, tzinfo=timezone.utc)
    start_time = end_time - timedelta(seconds=seconds)

    latency = latency_calc(start_time, sim, current_time=end_time)

    upper_bound = expected + 300

    assert expected <= latency < upper_bound


@pytest.mark.parametrize("seconds, expected", [
    (1, 1000),
    (2, 2000)
])
def test_latency_calc_none(seconds, expected):
    """Scenario 2: sim is not provided"""
    end_time = datetime(2026, 8, 12, 12, 0, 10, tzinfo=timezone.utc)
    start_time = end_time - timedelta(seconds=seconds)

    latency = latency_calc(start_time, current_time=end_time)

    upper_bound = expected+300
    
    # 3. Assert (1 second = 1000 milliseconds)
    assert expected <= latency < upper_bound, f"Expected latency around 1000 ms, got {latency} ms"


@pytest.mark.parametrize("seconds, expected", [
    (1, -4000),
    (2, -3000),
    (-1, 0)
])
def test_latency_calc_gbt(seconds, expected):
    """Scenario 3: sim is gbt"""
    sim = Stations.GBT
    end_time = datetime(2026, 8, 12, 12, 0, 10, tzinfo=timezone.utc)
    if seconds != -1:
        start_time = end_time - timedelta(seconds=seconds)
    else:
        start_time = -1
    latency = latency_calc(start_time, sim, current_time=end_time)

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
def test_config_func_VLBA():
    """Scenario 2: sim is a VLBA site"""
    sim = Stations.HN
    bootstrap = "12345"

    producer_topic, producer_config, consumer_topic, consumer_config = config_func(sim, bootstrap)

    assert producer_topic == "VLBA_notif"
    assert producer_config == {
            "bootstrap.servers": bootstrap,
            "message.max.bytes": 8388608,
            "client.id": f"{sim.name.lower()}-producer"
        }
    assert consumer_topic == ["GBT_data", "DSOC_notif"]
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

    producer_topic, producer_config, consumer_topic, consumer_config = config_func(sim, bootstrap)


    assert producer_topic == "DSOC_notif"
    assert producer_config == {
            "bootstrap.servers": bootstrap,
            "message.max.bytes": 8388608,
            "client.id": f"{sim.name.lower()}-producer"
        }
    assert consumer_topic == ["VLBA_notif"]
    assert consumer_config == {
            "bootstrap.servers": bootstrap,
            "fetch.max.bytes": 8388608,
            "session.timeout.ms": 45000,
            "client.id": f"{sim.name.lower()}-consumer",
            "group.id": f"{sim.name.lower()}-consumer-group",
            "auto.offset.reset": "earliest",
        }

    
def test_config_func_UI():
    """Scenario 4: kafka client is the UI. Producer only"""

    sim = Stations.UI
    bootstrap = "12345"

    topic, config = config_func(sim, bootstrap)

    assert topic == "user_input"
    assert config == {
            "bootstrap.servers": bootstrap,
            "message.max.bytes": 8388608,
            "client.id": f"{sim.name.lower()}-producer",
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
# X. create_file Test
# ==============================================================================

def test_create_file(tmp_path):
    file_path = tmp_path / "test.bin"

    create_file(file_path, file_mb=1)

    assert file_path.exists()
    assert file_path.stat().st_size == 1 * 1024 * 1024


# ==============================================================================
# X. delete_observation_data Test
# ==============================================================================

def test_delete_observation_data_exist(tmp_path):
    temp_file_name = "sample_data.bin"
    temp_file = tmp_path / temp_file_name
    temp_file.write_bytes(b"This is a test file")

    assert temp_file.exists()

    delete_observation_data(temp_file_name, dir=tmp_path)

    assert not temp_file.exists()


def test_delete_observation_data_not_exist(capsys, tmp_path):
    temp_file_name = "test_fail.bin"

    delete_observation_data(temp_file_name, dir=tmp_path)

    captured = capsys.readouterr()

    assert captured.out.strip() == f"File {temp_file_name} does not exists"
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
        ["etc", str(mock_frame_path), os.environ["ETD_DESTINATION"],],
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


# ==============================================================================
# 6. produce Test
# ==============================================================================

@patch("ngRadar_Website.utils.Producer")
def test_produce(mock_Producer):
    topic = "topic"
    config = "config"
    key = "key"
    value = "value"
    
    mock_producer = mock_Producer.return_value

    produce(topic, config, key, value)

    mock_Producer.assert_called_once_with(config)
    mock_producer.produce.assert_called_once_with(topic, key=key, value=value)
    mock_producer.flush.assert_called_once()


# ==============================================================================
# 7. record_transfer_event Test
# ==============================================================================

@patch("ngRadar_Website.utils.gbtEvent")
@patch("ngRadar_Website.utils.ETransferEvent")
def test_record_transfer_event(mock_etr_event, mock_gbt_event):

    mock_gbt_data = MagicMock()
    mock_gbt_data.object_id = "123"
    mock_gbt_data.target = "Venus"

    mock_gbt_event.objects.get.return_value = mock_gbt_data

    etr_record = MagicMock()
    mock_etr_event.objects.create.return_value = etr_record

    record_transfer_event(
        transfer_uuid="transfer-uuid",
        gbt_uuid="gbt-uuid",
        station=Stations.DSOC,
        status=Status.TRANSFERRED,
        num_bytes=2048,
        latency_ms=500,
        message="Test message")

    mock_gbt_event.objects.get.assert_called_once_with(uuid="gbt-uuid")
    mock_etr_event.objects.create.assert_called_once_with(
        transfer_uuid="transfer-uuid",
        gbt_uuid="gbt-uuid",
        object_id="123",
        target="Venus",
        station=Stations.DSOC,
        event_time=mock_etr_event.objects.create.call_args[1]['event_time'],
        latency_ms=500,
        num_bytes=2048,
        status=Status.TRANSFERRED,
        message="Test message")

# ==============================================================================
# 8. send_kafka_message Test
# ==============================================================================

@patch("ngRadar_Website.utils.produce")
@patch("ngRadar_Website.utils.datetime")
def test_send_kafka_message(mock_datetime, mock_produce):
    key=1
    producer_topic="test_topic"
    producer_config="test_config"
    transfer_uuid="test_transfer_uuid"
    gbt_uuid="test_gbt_uuid"
    status=Status.TRANSFERRING
    num_bytes=2048
    filename="mock.filename"
    message=1

    mock_produce.return_value = None

    fake_datetime = MagicMock()
    fake_datetime.isoformat.return_value = "2026-08-12T12:34:56+00:00"
    mock_datetime.now.return_value = fake_datetime

    send_kafka_message(
        key=key,
        producer_topic=producer_topic,
        producer_config=producer_config,
        transfer_uuid=transfer_uuid,
        gbt_uuid=gbt_uuid,
        status=status,
        num_bytes=num_bytes,
        filename=filename, 
        message=message,
    )

    mock_produce.assert_called_once_with(
        producer_topic,
        producer_config,
        key,
        '{"transfer_uuid": "test_transfer_uuid", "gbt_uuid": "test_gbt_uuid", "status": 4, "num_bytes": 2048, "filename": "mock.filename", "event_time": "2026-08-12T12:34:56+00:00", "message": 1, "stations": "Hancock (25-m, VLBA)"}',
    )


# ==============================================================================
# 9. get_folder_size Test
# ==============================================================================

def test_get_folder_size(tmp_path):
    root = tmp_path / "root"
    root.mkdir()

    # root/a.txt = 3 bytes
    (root / "a.txt").write_bytes(b"abc")

    # root/sub/b.bin = 5 bytes
    sub = root / "sub"
    sub.mkdir()
    (sub / "b.bin").write_bytes(b"12345")

    # root/sub2/c.dat = 0 bytes
    sub2 = root / "sub2"
    sub2.mkdir()
    (sub2 / "c.dat").write_bytes(b"")

    expected = 3 + 5 + 0
    assert get_folder_size(root) == expected


def test_get_folder_size_FileNotFoundError(tmp_path):
    missing = tmp_path / "does_not_exist"

    with pytest.raises(FileNotFoundError) as excinfo:
        get_folder_size(missing)

    # Optional: ensure it carries the same path object/message
    assert excinfo.value.args[0] == missing


# ==============================================================================
# 10. write_transfer_progress Test
# ==============================================================================

@patch("ngRadar_Website.utils.open")
@patch("ngRadar_Website.utils.json.dump")
@patch("ngRadar_Website.utils.os.replace")
def test_write_transfer_progress(
    mock_os_replace,
    mock_json,
    mock_open
    ):

    received_bytes = 100
    total_bytes = 200
    percent = "50.0"
    transfer_id = "11111111-1111-1111-1111-111111111111"

    mock_file = MagicMock()

    mock_open.return_value.__enter__.return_value = mock_file

    write_transfer_progress(
        received_bytes=received_bytes,
        total_bytes=total_bytes,
        percent=percent,
        transfer_id=transfer_id
    )


    mock_open.assert_called_once_with(
        "/service/mock_assets/progress.json.tmp",
        "w",
        encoding="utf-8",
    )

    expected = {
        "received_bytes": 100,
        "total_bytes": 200,
        "percent": "50.0",
        "transfer_id": "11111111-1111-1111-1111-111111111111",
    }

    mock_json.assert_called_once_with(
        expected,
        mock_file
    )

    mock_os_replace.assert_called_once_with(
        "/service/mock_assets/progress.json.tmp",
        "/service/mock_assets/progress.json"
    )