from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock
from ngRadar_Website.enums import Status, Stations, Message
import uuid
import pytest

# =============================================
# TEST THE STANDALONE FUNCTIONS FROM DSOC_SIM
# =============================================

# ==============================================================================
# IMPORTANT:
# Because we read "ngrok_endpoint.env" on import, we need to patch the Path globally
# before importing all the functions we want to test.
# ==============================================================================
mock_env_data = "BOOTSTRAP_SERVER=localhost:9092\nSOME_OTHER_VAR=value" 
with patch("pathlib.Path.read_text", return_value=mock_env_data):
    from ngRadar_Website.management.commands.dsoc_sim import (
        DB_import,
        DB_columns,
        publish_dsocEvents,
        create_img,
        save_image_to_seaweedfs,
        verify_incoming_transfer,
        track_etransfer_progress,
        process_msg,
    )

    
# ==============================================================================
# 1. DB_import Test
# ==============================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.gbtEvent")
def test_db_import_success(mock_gbt_event):
    """Scenario 1: Successfully retrieve and format data matching a UUID."""
    mock_record = ("obj_123", "Mars", "SineWave", datetime(2026, 1, 1, tzinfo=timezone.utc))
    
    # Mocking Django chain query syntax: .filter().values_list().first()
    mock_query = mock_gbt_event.objects.filter.return_value
    mock_values = mock_query.values_list.return_value
    mock_values.first.return_value = mock_record

    result = DB_import("9c85a7c7-0506-44f3-9792-63b1867c6f97") # random uuid I pulled from render DB to test with
    
    assert result == mock_record
    mock_gbt_event.objects.filter.assert_called_once_with(uuid="9c85a7c7-0506-44f3-9792-63b1867c6f97")


@patch("ngRadar_Website.management.commands.dsoc_sim.gbtEvent") # fake a gbtEvent record, let's you bypass having to connect to postres to test logic
def test_db_import_empty_result(mock_gbt_event):
    """Scenario 2: Returns None when no matching UUID exists in the table."""
    mock_gbt_event.objects.filter.return_value.values_list.return_value.first.return_value = None

    result = DB_import("non-existent-uuid")
    
    assert result is None

# ==============================================================================
# 2. DB_columns Test
# ==============================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.datetime")
def test_db_columns_mapping(mock_datetime):
    """Scenario 1: Verify correct structural mapping of tuple elements into fields."""
    fixed_now = datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = fixed_now
    
    gbt_data = ("obj_999", "Jupiter", "SquareWave", fixed_now)
    
    result = DB_columns(gbt_data)
    
    assert result["object_id"] == "obj_999"
    assert result["target"] == "Jupiter"
    assert result["event_time"] == fixed_now

# ==============================================================================
# 3. publish_dsocEvents COMPONENT TESTS
# ==============================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.dsocEvent") # fake a dsocEvent record, let's you bypass having to connect to postres to test logic
def test_publish_dsocEvents(mock_dsoc_event):
    """Scenario 1: Valid payload correctly creates and outputs the model instance."""
    mock_instance = MagicMock()
    mock_dsoc_event.objects.create.return_value = mock_instance

    image_key = "fake_key/img.png"
    num_bytes = 2048
    data = {}
    xmit_station = "XMIT_STATION"
    rcvr_station = "RCVR_STATION"
    transfer_uuid = "TRANSFER_UUID"

    record = publish_dsocEvents(image_key=image_key, num_bytes=num_bytes, data=data, xmit_station=xmit_station, rcvr_station=rcvr_station, transfer_uuid=transfer_uuid)

    assert record == mock_instance
    mock_dsoc_event.objects.create.assert_called_once_with(image_key='fake_key/img.png', num_bytes=2048, xmit_station='XMIT_STATION', rcvr_station='RCVR_STATION', transfer_uuid='TRANSFER_UUID', status=Status.COMPLETED)


@patch("ngRadar_Website.management.commands.dsoc_sim.dsocEvent")
def test_publish_dsocEvents_exception(mock_dsoc_event):
    """Scenario 2: Handled database crash returns None instead of crashing runtime."""
    mock_dsoc_event.objects.create.side_effect = Exception("DB Connection Timeout")

    image_key = "fake_key/img.png"
    num_bytes = 2048
    data = {}
    xmit_station = "XMIT_STATION"
    rcvr_station = "RCVR_STATION"
    transfer_uuid = "TRANSFER_UUID"

    record = publish_dsocEvents(image_key=image_key, num_bytes=num_bytes, data=data, xmit_station=xmit_station, rcvr_station=rcvr_station, transfer_uuid=transfer_uuid)

    assert record is None

# ==============================================================================
# 4. create_img Test
# ==============================================================================

def test_create_img_output():
    """Ensure the function returns a BytesIO object with non-zero content."""
    tx_waveform = "SineWave"
    img_file, num_bytes = create_img(tx_waveform)
    
    assert isinstance(img_file, bytes)
    assert num_bytes > 0
    assert num_bytes == len(img_file)
    assert img_file.startswith(b"\x89PNG\r\n\x1a\n") #ensures it is in PNG format


# ==============================================================================
# 5. save_image_to_seaweedfs Test
# ==============================================================================

@patch.dict(
    "os.environ",
    {
        "WEED_S3_INTERNAL_DOMAIN": "seaweedfs.fake.com",
        "WEED_S3_ACCESS_KEY": "fake_access_key",
        "WEED_S3_SECRET_KEY": "fake_key",
        "WEED_S3_BUCKET": "fake_bucket",
    },
)
@patch("ngRadar_Website.management.commands.dsoc_sim.create_s3_client") #fake the boto3 module which interacts with seaweedfs
@patch("ngRadar_Website.management.commands.dsoc_sim.upload_seaweedfs")
def test_save_image_to_seaweedfs_success(mock_upload, mock_s3):
    """Scenario 1: no errors"""
    #function inputs:
    target = "Venus"
    image_file = b"fake png bytes"
    dsoc_uuid = "12345"

    #creating the fake boto3.client:
    mock_instance = MagicMock()
    mock_s3.return_value = mock_instance

    image_key = "fake_key/img.png"
    mock_upload.return_value = image_key

    output = save_image_to_seaweedfs(target, image_file, dsoc_uuid)

    assert output == image_key
    mock_s3.assert_called_once()
    mock_upload.assert_called_once_with(mock_instance, f"ddm/Venus/12345.png", b"fake png bytes")


@patch("ngRadar_Website.management.commands.dsoc_sim.create_s3_client")
@patch("ngRadar_Website.management.commands.dsoc_sim.publish_status_obsEvents")
def test_save_image_to_seaweedfs_error(mock_publish, mock_s3):
    """Scenario 2: """
    #function inputs:
    target = "Venus"
    image_file = b"fake png bytes"
    dsoc_uuid = "12345"

    mock_s3.side_effect = Exception("Failed to connect.")

    output = save_image_to_seaweedfs(target, image_file, dsoc_uuid)

    assert output == False
    mock_s3.assert_called_once()
    mock_publish.assert_called_once_with(
            status=Status.FAILED,
            msg="Failed to connect to SeaweedFS.",
        )


# ==============================================================================
# 6. verify_incoming_transfer Test
# ==============================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.time.sleep")
def test_verify_incoming_transfer_success(mock_sleep):
    """Scenario 1: file is there, correct size"""
    incoming_file = MagicMock()
    incoming_file.is_file.return_value = True
    incoming_file.stat.return_value.st_size = 500
    expected_num_bytes = 500

    mock_sleep.return_value = None

    result = verify_incoming_transfer(
        incoming_file=incoming_file,
        expected_num_bytes=expected_num_bytes)

    assert result == expected_num_bytes
    mock_sleep.assert_not_called()

@patch("ngRadar_Website.management.commands.dsoc_sim.time.sleep")
def test_verify_incoming_transfer_nofile(mock_sleep):
    """Scenario 2: file not found"""
    incoming_file = MagicMock()
    incoming_file.is_file.return_value = False
    expected_num_bytes = 500

    mock_sleep.return_value = None

    with pytest.raises(RuntimeError) as exc_info:
        verify_incoming_transfer(
            incoming_file=incoming_file,
            expected_num_bytes=expected_num_bytes)

    assert mock_sleep.call_count == 10
    assert str(exc_info.value) == (f"Transfer verification failed for {incoming_file}. ""Expected 500 bytes.")



# ==============================================================================
# 8. process_msg Tests
# ==============================================================================

"""Scenario 1: VLBA_REQUEST_STORAGE incoming message. Clean run, no failure cases. Respond YES to storage check."""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.record_transfer_event")
@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.get_folder_size")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
def test_process_msg_VLBA_REQUEST_STORAGE(
    mock_json,
    mock_get_folder_size,
    mock_send_kafka_message,
    mock_record_transfer_event,
    monkeypatch,
):
    
    monkeypatch.setenv("DSOC_VOLUME_SIZE", "2")

    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = b'1'

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": str(transfer_uuid),
            "gbt_uuid": str(gbt_uuid),
            "status": 1,
            "status_label": "READY",
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "event_time": datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc),
            "message": 2,
            "stations": str("fake_station"),
        }

    mock_json.return_value = mock_payload
    mock_get_folder_size.return_value = "12345" # low bytes so storage check returns Yes

    process_msg(msg, producer_topic, producer_config)

    mock_get_folder_size.assert_called_once_with(Path("/dsoc/incoming"))
    mock_record_transfer_event.assert_called_once_with(
                                transfer_uuid="11111111-1111-1111-1111-111111111111",
                                gbt_uuid="22222222-2222-2222-2222-222222222222",
                                station=Stations.HN,
                                status=Status.READY,
                                num_bytes=2048,
                                message=f"DSOC made room to to accept the incoming data from {Stations.HN.label}",
                            )     
    mock_send_kafka_message.assert_called_once_with(
                            key = f"{Message.DSOC_RESPOND_STORAGE}", 
                            producer_topic=producer_topic,
                            producer_config=producer_config, 
                            transfer_uuid="11111111-1111-1111-1111-111111111111",
                            gbt_uuid="22222222-2222-2222-2222-222222222222",
                            status=1,
                            num_bytes=2048,
                            filename="fake_filename.png",
                            message="Yes",
                        )
#=====================================================================


"""Scenario 2: VLBA_REQUEST_STORAGE incoming message. Payload status == FAILED case."""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.record_transfer_event")
@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.get_folder_size")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
def test_process_msg_VLBA_REQUEST_STORAGE_FAILED(
    mock_json,
    mock_get_folder_size,
    mock_send_kafka_message,
    mock_record_transfer_event,
    monkeypatch,
):
    
    monkeypatch.setenv("DSOC_VOLUME_SIZE", "2")

    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = b'1'

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": str(transfer_uuid),
            "gbt_uuid": str(gbt_uuid),
            "status": 7,
            "status_label": "FAILED",
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "event_time": datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc),
            "message": 2,
            "stations": str("fake_station"),
        }

    mock_json.return_value = mock_payload

    process_msg(msg, producer_topic, producer_config)

    assert mock_get_folder_size.call_count == 0
    mock_record_transfer_event.assert_called_once_with(
                                transfer_uuid="11111111-1111-1111-1111-111111111111",
                                gbt_uuid="22222222-2222-2222-2222-222222222222",
                                station=Stations.HN,
                                status=Status.FAILED,
                                num_bytes=2048,
                                message=2,
                            )    
    assert mock_send_kafka_message.call_count == 0

#=====================================================================


"""Scenario 3: VLBA_REQUEST_STORAGE incoming message. Payload message == 15 FAILED case - meaning final storage check failed."""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.record_transfer_event")
@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.get_folder_size")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
def test_process_msg_VLBA_REQUEST_STORAGE_15(
    mock_json,
    mock_get_folder_size,
    mock_send_kafka_message,
    mock_record_transfer_event,
    monkeypatch,
):
    
    monkeypatch.setenv("DSOC_VOLUME_SIZE", "2")

    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = b'1'

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": str(transfer_uuid),
            "gbt_uuid": str(gbt_uuid),
            "status": 1,
            "status_label": "READY",
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "event_time": datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc),
            "message": 15,
            "stations": str("fake_station"),
        }

    mock_json.return_value = mock_payload
    mock_get_folder_size.return_value = "30000000000" # high bytes so storage_used+expected_num_bytes >= storage_limit

    process_msg(msg, producer_topic, producer_config)

    mock_get_folder_size.assert_called_once_with(Path("/dsoc/incoming"))
    mock_record_transfer_event.assert_called_once_with(
                                transfer_uuid="11111111-1111-1111-1111-111111111111",
                                gbt_uuid="22222222-2222-2222-2222-222222222222",
                                station=Stations.HN,
                                status=Status.FAILED,
                                num_bytes=2048,
                                message=f"DSOC does not have enough storage. Failed 15 times.",
                            )
    assert mock_send_kafka_message.call_count == 0

#=====================================================================


"""Scenario 4: VLBA_REQUEST_STORAGE incoming message. Payload message == 1 FAILED case - meaning DSOC is retrying a storage check."""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.record_transfer_event")
@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.get_folder_size")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
def test_process_msg_VLBA_REQUEST_STORAGE_1(
    mock_json,
    mock_get_folder_size,
    mock_send_kafka_message,
    mock_record_transfer_event,
    monkeypatch,
):
    
    monkeypatch.setenv("DSOC_VOLUME_SIZE", "2")

    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = b'1'

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": str(transfer_uuid),
            "gbt_uuid": str(gbt_uuid),
            "status": 1,
            "status_label": "READY",
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "event_time": datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc),
            "message": 1,
            "stations": str("fake_station"),
        }

    mock_json.return_value = mock_payload
    mock_get_folder_size.return_value = "30000000000" # high bytes so storage_used+expected_num_bytes >= storage_limit

    process_msg(msg, producer_topic, producer_config)

    mock_get_folder_size.assert_called_once_with(Path("/dsoc/incoming"))
    mock_record_transfer_event.assert_called_once_with(
                            transfer_uuid="11111111-1111-1111-1111-111111111111",
                            gbt_uuid="22222222-2222-2222-2222-222222222222",
                            station=Stations.HN,
                            status=Status.RETRYING,
                            num_bytes=2048,
                            message=f"DSOC does not have enough storage. Retrying...",
                        )
    mock_send_kafka_message.assert_called_once_with(
                            key = f"{Message.DSOC_RESPOND_STORAGE}", 
                            producer_topic=producer_topic,
                            producer_config=producer_config, 
                            transfer_uuid="11111111-1111-1111-1111-111111111111",
                            gbt_uuid="22222222-2222-2222-2222-222222222222",
                            status=1,
                            num_bytes=2048,
                            filename="fake_filename.png",
                            message=2,
                        )

#=====================================================================


"""Scenario 5: VLBA_TRANSFERRING incoming message. Clean run, no failure cases. Finish <verify and complete> logic."""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.dsoc_sim.publish_dsocEvents")
@patch("ngRadar_Website.management.commands.dsoc_sim.save_image_to_seaweedfs")
@patch("ngRadar_Website.management.commands.dsoc_sim.create_img")
@patch("ngRadar_Website.management.commands.dsoc_sim.DB_columns")
@patch("ngRadar_Website.management.commands.dsoc_sim.latency_calc")
@patch("ngRadar_Website.management.commands.dsoc_sim.DB_import")
@patch("ngRadar_Website.management.commands.dsoc_sim.verify_incoming_transfer")
@patch("ngRadar_Website.management.commands.dsoc_sim.track_etransfer_progress")
@patch("ngRadar_Website.management.commands.dsoc_sim.record_transfer_event")
@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
def test_process_msg_VLBA_TRANSFERRING(
    mock_json,
    mock_send_kafka_message,
    mock_record_transfer_event,
    mock_track_etransfer_progress,
    mock_verify_incoming_transfer,
    mock_DB_import,
    mock_latency_calc,
    mock_DB_columns,
    mock_create_img,
    mock_save_image_to_seaweedfs,
    mock_publish_DB,
    mock_uuid
):
    
    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = b'3'

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": str(transfer_uuid),
            "gbt_uuid": str(gbt_uuid),
            "status": 1,
            "status_label": "READY",
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "event_time": datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc),
            "message": 2,
            "stations": str("fake_station"),
        }
    
    #pretend that, given the fake uuid, this data is extracted from the DB:
    mock_gbt_data = (
        "obj001",
        "Venus",
        "SineWave",
        datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)
    )

    mock_json.return_value = mock_payload
    mock_DB_import.return_value = mock_gbt_data
    mock_latency_calc.return_value = 100

    mock_data = MagicMock()
    mock_DB_columns.return_value = mock_data

    img_file = b"bytes"
    num_bytes = 500
    mock_create_img.return_value = img_file, num_bytes

    mock_uuid.return_value = "54321"

    image_key = f"ddm/'Venus'/54321.png"
    mock_save_image_to_seaweedfs.return_value = image_key

    process_msg(msg, producer_topic, producer_config)

    mock_track_etransfer_progress.assert_called_once_with(mock_payload, Path("/dsoc/incoming") / f"{mock_payload['transfer_uuid']}.bin")
    assert mock_record_transfer_event.call_count == 3
    mock_verify_incoming_transfer.assert_called_once_with( 
                    incoming_file=Path("/dsoc/incoming") / f"{mock_payload['transfer_uuid']}.bin",
                    expected_num_bytes=mock_payload["num_bytes"],
                )      
    mock_DB_import.assert_called_once_with(str(uuid.UUID("22222222-2222-2222-2222-222222222222")))
    mock_latency_calc.assert_called_once_with(datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc))
    mock_DB_columns.assert_called_once_with(mock_gbt_data)
    mock_create_img.assert_called_once_with("SineWave")
    mock_uuid.assert_called_once()
    mock_save_image_to_seaweedfs.assert_called_once_with(
                    "Venus",
                    b"bytes",
                    "54321",
                )
    mock_publish_DB.assert_called_once_with(
                    image_key=f"ddm/'Venus'/54321.png",
                    num_bytes=500,
                    data=mock_data,
                    xmit_station=Stations.GBT,
                    rcvr_station=Stations.HN,
                    transfer_uuid="11111111-1111-1111-1111-111111111111",
                )
    mock_send_kafka_message.assert_called_once_with(
                    key = f"{Message.VLBA_DELETE}", 
                    producer_topic=producer_topic,
                    producer_config=producer_config, 
                    transfer_uuid="11111111-1111-1111-1111-111111111111",
                    gbt_uuid="22222222-2222-2222-2222-222222222222",
                    status=1,
                    num_bytes=2048,
                    filename="fake_filename.png",
                    message="Processing complete. Delete your raw data.",
                )
#=====================================================================


"""Scenario 6: VLBA_TRANSFERRING incoming message. Verify incoming file FAILED case."""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.publish_dsocEvents")
@patch("ngRadar_Website.management.commands.dsoc_sim.save_image_to_seaweedfs")
@patch("ngRadar_Website.management.commands.dsoc_sim.create_img")
@patch("ngRadar_Website.management.commands.dsoc_sim.DB_columns")
@patch("ngRadar_Website.management.commands.dsoc_sim.latency_calc")
@patch("ngRadar_Website.management.commands.dsoc_sim.DB_import")
@patch("ngRadar_Website.management.commands.dsoc_sim.verify_incoming_transfer")
@patch("ngRadar_Website.management.commands.dsoc_sim.track_etransfer_progress")
@patch("ngRadar_Website.management.commands.dsoc_sim.record_transfer_event")
@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
def test_process_msg_VLBA_TRANSFERRING_verificationFAILED(
    mock_json,
    mock_send_kafka_message,
    mock_record_transfer_event,
    mock_track_etransfer_progress,
    mock_verify_incoming_transfer,
    mock_DB_import,
    mock_latency_calc,
    mock_DB_columns,
    mock_create_img,
    mock_save_image_to_seaweedfs,
    mock_publish_DB
):
    
    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = b'3'

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": str(transfer_uuid),
            "gbt_uuid": str(gbt_uuid),
            "status": 1,
            "status_label": "READY",
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "event_time": datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc),
            "message": 2,
            "stations": str("fake_station"),
        }

    mock_json.return_value = mock_payload
    mock_verify_incoming_transfer.side_effect = RuntimeError
    
    process_msg(msg, producer_topic, producer_config)

    mock_track_etransfer_progress.assert_called_once_with(mock_payload, Path("/dsoc/incoming") / f"{mock_payload['transfer_uuid']}.bin")
    assert mock_record_transfer_event.call_count == 3
    mock_verify_incoming_transfer.assert_called_once_with( 
                incoming_file=Path("/dsoc/incoming") / f"{mock_payload['transfer_uuid']}.bin",
                expected_num_bytes=mock_payload["num_bytes"],
            )    
    assert mock_DB_import.call_count == 0
    assert mock_latency_calc.call_count == 0
    assert mock_DB_columns.call_count == 0
    assert mock_create_img.call_count == 0
    assert mock_save_image_to_seaweedfs.call_count == 0
    assert mock_publish_DB.call_count == 0
    assert mock_send_kafka_message.call_count == 0

#=====================================================================


"""Scenario 7: VLBA_TRANSFERRING incoming message. Image processing FAILED case. Arbitrarily picking save_to_seaweedfs to fail"""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.dsoc_sim.publish_dsocEvents")
@patch("ngRadar_Website.management.commands.dsoc_sim.save_image_to_seaweedfs")
@patch("ngRadar_Website.management.commands.dsoc_sim.create_img")
@patch("ngRadar_Website.management.commands.dsoc_sim.DB_columns")
@patch("ngRadar_Website.management.commands.dsoc_sim.latency_calc")
@patch("ngRadar_Website.management.commands.dsoc_sim.DB_import")
@patch("ngRadar_Website.management.commands.dsoc_sim.verify_incoming_transfer")
@patch("ngRadar_Website.management.commands.dsoc_sim.track_etransfer_progress")
@patch("ngRadar_Website.management.commands.dsoc_sim.record_transfer_event")
@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
def test_process_msg_VLBA_TRANSFERRING_processingFAILED(
    mock_json,
    mock_send_kafka_message,
    mock_record_transfer_event,
    mock_track_etransfer_progress,
    mock_verify_incoming_transfer,
    mock_DB_import,
    mock_latency_calc,
    mock_DB_columns,
    mock_create_img,
    mock_save_image_to_seaweedfs,
    mock_publish_DB,
    mock_uuid
):
    
    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = b'3'

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": str(transfer_uuid),
            "gbt_uuid": str(gbt_uuid),
            "status": 1,
            "status_label": "READY",
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "event_time": datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc),
            "message": 2,
            "stations": str("fake_station"),
        }
    
    #pretend that, given the fake uuid, this data is extracted from the DB:
    mock_gbt_data = (
        "obj001",
        "Venus",
        "SineWave",
        datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)
    )

    mock_json.return_value = mock_payload
    mock_DB_import.return_value = mock_gbt_data
    mock_latency_calc.return_value = 100

    mock_data = MagicMock()
    mock_DB_columns.return_value = mock_data

    img_file = b"bytes"
    num_bytes = 500
    mock_create_img.return_value = img_file, num_bytes

    mock_uuid.return_value = "54321"

    mock_save_image_to_seaweedfs.side_effect = RuntimeError
    
    process_msg(msg, producer_topic, producer_config)

    mock_track_etransfer_progress.assert_called_once_with(mock_payload, Path("/dsoc/incoming") / f"{mock_payload['transfer_uuid']}.bin")
    assert mock_record_transfer_event.call_count == 3
    mock_verify_incoming_transfer.assert_called_once_with( 
                incoming_file=Path("/dsoc/incoming") / f"{mock_payload['transfer_uuid']}.bin",
                expected_num_bytes=mock_payload["num_bytes"],
            )    
    mock_DB_import.assert_called_once_with(str(uuid.UUID("22222222-2222-2222-2222-222222222222")))
    mock_latency_calc.assert_called_once_with(datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc))
    mock_DB_columns.assert_called_once_with(mock_gbt_data)
    mock_create_img.assert_called_once_with("SineWave")
    mock_uuid.assert_called_once()
    mock_save_image_to_seaweedfs.assert_called_once_with(
                "Venus",
                b"bytes",
                "54321",
            )    
    assert mock_publish_DB.call_count == 0
    assert mock_send_kafka_message.call_count == 0

#=====================================================================

"""Scenario 8: VLBA_TRANSFERRING incoming message. track_etransfer_progress FAILED case."""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.publish_dsocEvents")
@patch("ngRadar_Website.management.commands.dsoc_sim.save_image_to_seaweedfs")
@patch("ngRadar_Website.management.commands.dsoc_sim.create_img")
@patch("ngRadar_Website.management.commands.dsoc_sim.DB_columns")
@patch("ngRadar_Website.management.commands.dsoc_sim.latency_calc")
@patch("ngRadar_Website.management.commands.dsoc_sim.DB_import")
@patch("ngRadar_Website.management.commands.dsoc_sim.verify_incoming_transfer")
@patch("ngRadar_Website.management.commands.dsoc_sim.track_etransfer_progress")
@patch("ngRadar_Website.management.commands.dsoc_sim.record_transfer_event")
@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
def test_process_msg_VLBA_TRANSFERRING_trackingFAILED(
    mock_json,
    mock_send_kafka_message,
    mock_record_transfer_event,
    mock_track_etransfer_progress,
    mock_verify_incoming_transfer,
    mock_DB_import,
    mock_latency_calc,
    mock_DB_columns,
    mock_create_img,
    mock_save_image_to_seaweedfs,
    mock_publish_DB
):
    
    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = b'3'

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": str(transfer_uuid),
            "gbt_uuid": str(gbt_uuid),
            "status": 1,
            "status_label": "READY",
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "event_time": datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc),
            "message": 2,
            "stations": str("fake_station"),
        }

    mock_json.return_value = mock_payload
    mock_track_etransfer_progress.side_effect = Exception("Failed.")
    
    process_msg(msg, producer_topic, producer_config)

    mock_track_etransfer_progress.assert_called_once_with(mock_payload, Path("/dsoc/incoming") / f"{mock_payload['transfer_uuid']}.bin")
    assert mock_record_transfer_event.call_count == 0
    assert mock_verify_incoming_transfer.call_count == 0
    assert mock_DB_import.call_count == 0
    assert mock_latency_calc.call_count == 0
    assert mock_DB_columns.call_count == 0
    assert mock_create_img.call_count == 0
    assert mock_save_image_to_seaweedfs.call_count == 0
    assert mock_publish_DB.call_count == 0

    assert mock_send_kafka_message.call_count == 0
#=====================================================================


"""Scenario 9: VLBA_TRANSFERRING incoming message. Image key is FALSE case. Arbitrarily picking save_to_seaweedfs to fail"""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.dsoc_sim.publish_dsocEvents")
@patch("ngRadar_Website.management.commands.dsoc_sim.save_image_to_seaweedfs")
@patch("ngRadar_Website.management.commands.dsoc_sim.create_img")
@patch("ngRadar_Website.management.commands.dsoc_sim.DB_columns")
@patch("ngRadar_Website.management.commands.dsoc_sim.latency_calc")
@patch("ngRadar_Website.management.commands.dsoc_sim.DB_import")
@patch("ngRadar_Website.management.commands.dsoc_sim.verify_incoming_transfer")
@patch("ngRadar_Website.management.commands.dsoc_sim.track_etransfer_progress")
@patch("ngRadar_Website.management.commands.dsoc_sim.record_transfer_event")
@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
def test_process_msg_VLBA_TRANSFERRING_image_falseFAILED(
    mock_json,
    mock_send_kafka_message,
    mock_record_transfer_event,
    mock_track_etransfer_progress,
    mock_verify_incoming_transfer,
    mock_DB_import,
    mock_latency_calc,
    mock_DB_columns,
    mock_create_img,
    mock_save_image_to_seaweedfs,
    mock_publish_DB,
    mock_uuid
):
    
    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = b'3'

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": str(transfer_uuid),
            "gbt_uuid": str(gbt_uuid),
            "status": 1,
            "status_label": "READY",
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "event_time": datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc),
            "message": 2,
            "stations": str("fake_station"),
        }
    
    #pretend that, given the fake uuid, this data is extracted from the DB:
    mock_gbt_data = (
        "obj001",
        "Venus",
        "SineWave",
        datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)
    )

    mock_json.return_value = mock_payload
    mock_DB_import.return_value = mock_gbt_data
    mock_latency_calc.return_value = 100

    mock_data = MagicMock()
    mock_DB_columns.return_value = mock_data

    img_file = b"bytes"
    num_bytes = 500
    mock_create_img.return_value = img_file, num_bytes

    mock_uuid.return_value = "54321"

    mock_save_image_to_seaweedfs.return_value = False
    
    process_msg(msg, producer_topic, producer_config)

    mock_track_etransfer_progress.assert_called_once_with(mock_payload, Path("/dsoc/incoming") / f"{mock_payload['transfer_uuid']}.bin")
    assert mock_record_transfer_event.call_count == 2
    mock_verify_incoming_transfer.assert_called_once_with( 
                incoming_file=Path("/dsoc/incoming") / f"{mock_payload['transfer_uuid']}.bin",
                expected_num_bytes=mock_payload["num_bytes"],
            )
    mock_DB_import.assert_called_once_with(str(uuid.UUID("22222222-2222-2222-2222-222222222222")))
    mock_latency_calc.assert_called_once_with(datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc))
    mock_DB_columns.assert_called_once_with(mock_gbt_data)
    mock_create_img.assert_called_once_with("SineWave")
    mock_uuid.assert_called_once()
    mock_save_image_to_seaweedfs.assert_called_once_with(
                "Venus",
                b"bytes",
                "54321",
            )
    assert mock_publish_DB.call_count == 0
    assert mock_send_kafka_message.call_count == 0

#=====================================================================


"""Scenario 10: incoming message is not VLBA_REQUEST_STORAGE or VLBA_TRANSFERRING."""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.dsoc_sim.publish_dsocEvents")
@patch("ngRadar_Website.management.commands.dsoc_sim.save_image_to_seaweedfs")
@patch("ngRadar_Website.management.commands.dsoc_sim.create_img")
@patch("ngRadar_Website.management.commands.dsoc_sim.DB_columns")
@patch("ngRadar_Website.management.commands.dsoc_sim.latency_calc")
@patch("ngRadar_Website.management.commands.dsoc_sim.DB_import")
@patch("ngRadar_Website.management.commands.dsoc_sim.verify_incoming_transfer")
@patch("ngRadar_Website.management.commands.dsoc_sim.track_etransfer_progress")
@patch("ngRadar_Website.management.commands.dsoc_sim.record_transfer_event")
@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
def test_process_msg_invalid_key(
    mock_json,
    mock_send_kafka_message,
    mock_record_transfer_event,
    mock_track_etransfer_progress,
    mock_verify_incoming_transfer,
    mock_DB_import,
    mock_latency_calc,
    mock_DB_columns,
    mock_create_img,
    mock_save_image_to_seaweedfs,
    mock_publish_DB,
    mock_uuid
):
    
    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = b'2'

    producer_topic = MagicMock()
    producer_config = MagicMock()
    
    process_msg(msg, producer_topic, producer_config)

    assert mock_track_etransfer_progress.call_count == 0
    assert mock_record_transfer_event.call_count == 0
    assert mock_verify_incoming_transfer.call_count == 0
    assert mock_DB_import.call_count == 0
    assert mock_latency_calc.call_count == 0
    assert mock_DB_columns.call_count == 0
    assert mock_create_img.call_count == 0
    assert mock_uuid.call_count == 0
    assert mock_save_image_to_seaweedfs.call_count == 0
    assert mock_publish_DB.call_count == 0
    assert mock_send_kafka_message.call_count == 0


# ==============================================================================
# 9. track_etransfer_progress Tests
# ==============================================================================

"""Scenario 1: Clean run, no fails"""
#===================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.time.sleep", return_value=None)
@patch("ngRadar_Website.management.commands.dsoc_sim.write_transfer_progress")
@patch("ngRadar_Website.management.commands.dsoc_sim.ETransferEvent")
def test_track_etransfer_progress(
    mock_etransfer_event,
    mock_write_transfer_progress,
    mock_sleep
):
    payload = {
        "transfer_uuid": "11111111-1111-1111-1111-111111111111",
        "num_bytes": 1000,
    }

    incoming_file = MagicMock()
    incoming_file.exists.return_value = True
    incoming_file.stat.return_value.st_size = 1000

    # Build the full ORM chain:
    # objects.filter(...).order_by(...).values_list(...).first()
    mock_values_list = MagicMock()
    mock_values_list.first.return_value = Status.TRANSFERRING

    mock_order_by = MagicMock()
    mock_order_by.values_list.return_value = mock_values_list

    mock_filter = MagicMock()
    mock_filter.order_by.return_value = mock_order_by

    mock_etransfer_event.objects.filter.return_value = mock_filter

    track_etransfer_progress(payload, incoming_file=incoming_file)

    mock_write_transfer_progress.assert_any_call(
        received_bytes=0,
        total_bytes=0,
        percent=0,
        transfer_id=0,
    )

    # Assert that it wrote completion progress (received_bytes == num_bytes)
    mock_write_transfer_progress.assert_any_call(
        received_bytes=1000,
        total_bytes=1000,
        percent="100.0",
        transfer_id=payload["transfer_uuid"],
    )

    # Assert: while condition had status TRANSFERRING at least once
    assert mock_etransfer_event.objects.filter.call_count >= 1
    incoming_file.exists.assert_called()


#===================================================================
"""Scenario 2: Status changed to FAILED mid-etransfer. Failed run"""
#===================================================================

from unittest.mock import MagicMock, patch

from ngRadar_Website.management.commands.dsoc_sim import track_etransfer_progress, Status


@patch("ngRadar_Website.management.commands.dsoc_sim.time.sleep", return_value=None)
@patch("ngRadar_Website.management.commands.dsoc_sim.write_transfer_progress")
@patch("ngRadar_Website.management.commands.dsoc_sim.ETransferEvent")
def test_track_etransfer_progress_status_FAILED(
    mock_etransfer_event,
    mock_write_transfer_progress,
    mock_sleep,
):
    payload = {
        "transfer_uuid": "11111111-1111-1111-1111-111111111111",
        "num_bytes": 1000,
    }

    incoming_file = MagicMock()
    incoming_file.exists.return_value = True

    incoming_file.stat.return_value.st_size = 200 # less than num_bytes so while loop can't complete on its own, status change must trigger exit.

    mock_values_list = MagicMock()
    mock_values_list.first.side_effect = [
        Status.TRANSFERRING,  # while condition enters loop
        Status.FAILED,       # while condition fails next iteration, exits loop
        Status.FAILED,       # post-loop FAILED check => raise
    ]

    mock_order_by = MagicMock()
    mock_order_by.values_list.return_value = mock_values_list

    mock_filter = MagicMock()
    mock_filter.order_by.return_value = mock_order_by

    mock_etransfer_event.objects.filter.return_value = mock_filter

    with pytest.raises(ValueError, match="FAILED"):
        track_etransfer_progress(payload, incoming_file=incoming_file)


#===================================================================
"""Scenario 3: Status changed to something else mid e-transfer. Failed run """
#===================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.time.sleep", return_value=None)
@patch("ngRadar_Website.management.commands.dsoc_sim.write_transfer_progress")
@patch("ngRadar_Website.management.commands.dsoc_sim.ETransferEvent")
def test_track_etransfer_progress_status_OTHER(
    mock_etransfer_event,
    mock_write_transfer_progress,
    mock_sleep,
):
    payload = {
        "transfer_uuid": "11111111-1111-1111-1111-111111111111",
        "num_bytes": 1000,
    }

    incoming_file = MagicMock()
    incoming_file.exists.return_value = True
    incoming_file.stat.return_value.st_size = 200  # != num_bytes should raise at end

    # Use a status that is "something else" (not TRANSFERRING and not FAILED).
    other_status = MagicMock(name="other_status")
    other_status != Status.TRANSFERRING
    other_status != Status.FAILED

    mock_values_list = MagicMock()
    mock_values_list.first.side_effect = [
        Status.TRANSFERRING,  # iteration 1 while check: enter loop
        other_status,         # iteration 2 while check: exit loop
        other_status,         # post-loop FAILED check: not FAILED
    ]

    mock_order_by = MagicMock()
    mock_order_by.values_list.return_value = mock_values_list

    mock_filter = MagicMock()
    mock_filter.order_by.return_value = mock_order_by

    mock_etransfer_event.objects.filter.return_value = mock_filter

    with pytest.raises(ValueError, match="progress has halted"):
        track_etransfer_progress(payload, incoming_file=incoming_file)

    # confirm we wrote at least one non-reset progress update before exiting
    mock_write_transfer_progress.assert_any_call(
        received_bytes=200,
        total_bytes=1000,
        percent="20.0", 
        transfer_id=payload["transfer_uuid"],
    )
