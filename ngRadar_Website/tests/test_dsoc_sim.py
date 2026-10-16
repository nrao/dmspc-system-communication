from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import call, patch, MagicMock
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
        create_img,
        save_image_to_seaweedfs,
        verify_incoming_transfer,
        process_msg,
    )


# ==============================================================================
# 1. create_img Test
# ==============================================================================

def test_create_img_output():
    """Ensure the function returns a BytesIO object with non-zero content."""
    station = 94
    tx_waveform = "SineWave"
    waveform_requester = "user"
    img_file, num_bytes = create_img(station, tx_waveform, waveform_requester=waveform_requester)
    
    assert isinstance(img_file, bytes)
    assert num_bytes > 0
    assert num_bytes == len(img_file)
    assert img_file.startswith(b"\x89PNG\r\n\x1a\n") #ensures it is in PNG format


# ==============================================================================
# 2. save_image_to_seaweedfs Test
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
@patch("ngRadar_Website.management.commands.dsoc_sim.upload_seaweedfs")
def test_save_image_to_seaweedfs_error(mock_upload, mock_s3):
    """Scenario 2: error"""
    #function inputs:
    target = "Venus"
    image_file = b"fake png bytes"
    dsoc_uuid = "12345"

    mock_s3.side_effect = Exception("Failed to connect.")

    with pytest.raises(RuntimeError) as exc_info:
        save_image_to_seaweedfs(target, image_file, dsoc_uuid)

    mock_s3.assert_called_once()
    mock_upload.assert_not_called()


# ==============================================================================
# 3. verify_incoming_transfer Test
# ==============================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.time.sleep")
def test_verify_incoming_transfer_success(mock_sleep, mock_kafka):
    """Scenario 1: file is there, correct size"""
    incoming_file = MagicMock()
    incoming_file.is_file.return_value = True
    incoming_file.stat.return_value.st_size = 500

    mock_sleep.return_value = None

    result = verify_incoming_transfer(
        incoming_file=incoming_file,
        expected_num_bytes=500,
        producer_topic = "topic",
        producer_config = "config",
        gbt_event_time = "2023-01-01T00:00:00Z",
        gbt_uuid = "gbt uuid",
        object_id = "object_id",
        target = "target",
        waveform_requester = "user",
        tx_waveform = "SineWave",
        rec_waveform = "SineWave",
        filename = "fake_filename.png",
        transfer_uuid = "12345",
        vlba_station = Stations.PT)

    assert result == 500
    mock_sleep.assert_not_called()
    mock_kafka.assert_called_once()

@patch("ngRadar_Website.management.commands.dsoc_sim.time.sleep")
def test_verify_incoming_transfer_nofile(mock_sleep):
    """Scenario 2: file not found"""
    incoming_file = MagicMock()
    incoming_file.is_file.return_value = True
    incoming_file.stat.return_value.st_size = 500

    mock_sleep.return_value = None

    with pytest.raises(RuntimeError) as exc_info:
        verify_incoming_transfer(
            incoming_file=incoming_file,
            expected_num_bytes=400,
            producer_topic = "topic",
            producer_config = "config",
            gbt_event_time = "2023-01-01T00:00:00Z",
            gbt_uuid = "gbt uuid",
            object_id = "object_id",
            target = "target",
            waveform_requester = "user",
            tx_waveform = "SineWave",
            rec_waveform = "SineWave",
            filename = "fake_filename.png",
            transfer_uuid = "12345",
            vlba_station = Stations.PT)

    assert mock_sleep.call_count == 10
    assert str(exc_info.value) == ("Transfer verification failed for "
            f"{incoming_file}. Expected "
            "400 bytes.")


# ==============================================================================
# 4. process_msg Tests
# ==============================================================================

"""Scenario 1: VLBA_REQUEST_STORAGE incoming message. Clean run, no failure cases. Respond YES to storage check."""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.get_folder_size")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
def test_process_msg_VLBA_REQUEST_STORAGE(
    mock_json,
    mock_get_folder_size,
    mock_send_kafka_message,
    monkeypatch,
):
    
    monkeypatch.setenv("DSOC_VOLUME_SIZE", "2")

    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = str(
        Message.VLBA_REQUEST_STORAGE.value
        ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": (transfer_uuid),
            "gbt_uuid": (gbt_uuid),
            "object_id": str("fake_object_id"),
            "target": str("fake_target"),
            "waveform_requester": str("user"),
            "tx_waveform": str("fake_tx_waveform"),
            "rec_waveform": str("fake_rec_waveform"),
            "gbt_event_time": str("2026-07-15T12:00:00+00:00"),
            "status": 1,
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "message": 2,
            "station": Stations.PT,
            "retry_count": 5,
        }

    mock_json.return_value = mock_payload
    mock_get_folder_size.return_value = "12345" # low bytes so storage check returns Yes

    mock_send_kafka_message.return_value = MagicMock()

    process_msg(msg, producer_topic, producer_config)

    mock_get_folder_size.assert_called_once_with(Path("/dsoc/incoming"))
    mock_send_kafka_message.assert_called_once_with(
                producer_topic=(producer_topic),
                producer_config=(producer_config),
                waveform_requester=str("user"),
                message_type=(Message.DSOC_RESPOND_STORAGE),
                transfer_uuid=(transfer_uuid),
                gbt_uuid=gbt_uuid,
                gbt_event_time=str("2026-07-15T12:00:00+00:00"),
                station=Stations.PT,
                status=Status.READY,
                object_id=str("fake_object_id"),
                target=str("fake_target"),
                tx_waveform=str("fake_tx_waveform"),
                rec_waveform=(str("fake_rec_waveform")),
                num_bytes=(2048),
                filename=str("fake_filename.png"),
                retry_count=(5),
                xmit_station=(Stations.GBT),
                rcvr_station=(Stations.PT),

                message=f"DSOC reponded that it has enough storage. {Stations.PT.name} may begin the etransfer.",
            )
#=====================================================================


"""Scenario 2: VLBA_REQUEST_STORAGE incoming message. Not enough storage, Max retries NOT yet reached"""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.get_folder_size")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
def test_process_msg_VLBA_REQUEST_STORAGE_Retry(
        mock_json,
        mock_get_folder_size,
        mock_send_kafka_message,
        monkeypatch,
    ):
    
    monkeypatch.setenv("DSOC_VOLUME_SIZE", "1")

    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = str(
        Message.VLBA_REQUEST_STORAGE.value
        ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": (transfer_uuid),
            "gbt_uuid": (gbt_uuid),
            "object_id": str("fake_object_id"),
            "target": str("fake_target"),
            "waveform_requester": str("user"),
            "tx_waveform": str("fake_tx_waveform"),
            "rec_waveform": str("fake_rec_waveform"),
            "gbt_event_time": str("2026-07-15T12:00:00+00:00"),
            "status": 1,
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "message": 2,
            "station": Stations.PT,
            "retry_count": 5,
        }

    mock_json.return_value = mock_payload
    mock_get_folder_size.return_value = "99999999999999999" # high bytes so storage check returns No

    mock_send_kafka_message.return_value = MagicMock()

    process_msg(msg, producer_topic, producer_config)

    mock_get_folder_size.assert_called_once_with(Path("/dsoc/incoming"))
    mock_send_kafka_message.assert_called_once_with(
                producer_topic=(producer_topic),
                producer_config=(producer_config),
                waveform_requester=str("user"),
                message_type=(Message.DSOC_RESPOND_STORAGE),
                transfer_uuid=(transfer_uuid),
                gbt_uuid=gbt_uuid,
                gbt_event_time=str("2026-07-15T12:00:00+00:00"),
                station=Stations.DSOC,
                status=Status.RETRYING,
                object_id=str("fake_object_id"),
                target=str("fake_target"),
                tx_waveform=str("fake_tx_waveform"),
                rec_waveform=(str("fake_rec_waveform")),
                num_bytes=(2048),
                filename=str("fake_filename.png"),
                retry_count=(6),
                xmit_station=(Stations.GBT),
                rcvr_station=(Stations.PT),
                message=f"{Stations.PT.name} requested a storage check at DSOC. DSOC responded that it does not have enough storage and cannot begin the etransfer.",
                            )

#=====================================================================


"""Scenario 3: VLBA_REQUEST_STORAGE incoming message. Not enough storage, Max retries is reached"""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.get_folder_size")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
def test_process_msg_VLBA_REQUEST_STORAGE_Failed(
        mock_json,
        mock_get_folder_size,
        mock_send_kafka_message,
        monkeypatch,
    ):
    
    monkeypatch.setenv("DSOC_VOLUME_SIZE", "1")

    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = str(
        Message.VLBA_REQUEST_STORAGE.value
        ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": (transfer_uuid),
            "gbt_uuid": (gbt_uuid),
            "object_id": str("fake_object_id"),
            "target": str("fake_target"),
            "waveform_requester": str("user"),
            "tx_waveform": str("fake_tx_waveform"),
            "rec_waveform": str("fake_rec_waveform"),
            "gbt_event_time": str("2026-07-15T12:00:00+00:00"),
            "status": 1,
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "message": 2,
            "station": Stations.PT,
            "retry_count": 15,
        }

    mock_json.return_value = mock_payload
    mock_get_folder_size.return_value = "99999999999999999" # high bytes so storage check returns No

    mock_send_kafka_message.return_value = MagicMock()

    process_msg(msg, producer_topic, producer_config)

    mock_get_folder_size.assert_called_once_with(Path("/dsoc/incoming"))
    mock_send_kafka_message.assert_called_once_with(
                producer_topic=(producer_topic),
                producer_config=(producer_config),
                waveform_requester=str("user"),
                message_type=(Message.DSOC_RESPOND_STORAGE),
                transfer_uuid=(transfer_uuid),
                gbt_uuid=gbt_uuid,
                gbt_event_time=str("2026-07-15T12:00:00+00:00"),
                station=Stations.DSOC,
                status=Status.FAILED,
                object_id=str("fake_object_id"),
                target=str("fake_target"),
                tx_waveform=str("fake_tx_waveform"),
                rec_waveform=(str("fake_rec_waveform")),
                num_bytes=(2048),
                filename=str("fake_filename.png"),
                retry_count=(16),
                xmit_station=(Stations.GBT),
                rcvr_station=(Stations.PT),
                message=(
                            f"{Stations.PT.name} requested a storage check at DSOC. "
                            f"DSOC responded that it does not have enough storage and cannot begin the etransfer."
                            f"Failed after "
                            f"{16} "
                            "storage checks."
                        ),
                    )

#=====================================================================


"""Scenario 4: PROGRESS_COMPLETE incoming message. Clean run, no failure cases. Finish <verify and complete> logic."""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
@patch("ngRadar_Website.management.commands.dsoc_sim.verify_incoming_transfer")
@patch("ngRadar_Website.management.commands.dsoc_sim.latency_calc")
@patch("ngRadar_Website.management.commands.dsoc_sim.create_img")
@patch("ngRadar_Website.management.commands.dsoc_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.dsoc_sim.save_image_to_seaweedfs")
def test_process_msg_Message_PROGRESS_COMPLETE_value_success(
        mock_save_img,
        mock_uuid,
        mock_create_img,
        mock_latency,
        mock_verify,
        mock_json,
        mock_send_kafka_message,
        monkeypatch,
    ):
    
    monkeypatch.setenv("DSOC_VOLUME_SIZE", "1")

    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = str(
        Message.PROGRESS_COMPLETE.value
        ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": (transfer_uuid),
            "gbt_uuid": (gbt_uuid),
            "object_id": str("fake_object_id"),
            "target": str("fake_target"),
            "waveform_requester": str("user"),
            "tx_waveform": str("fake_tx_waveform"),
            "rec_waveform": str("fake_rec_waveform"),
            "gbt_event_time": str("2026-07-15T12:00:00+00:00"),
            "status": 1,
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "message": 2,
            "station": Stations.PT,
            "retry_count": 5,
        }

    mock_json.return_value = mock_payload

    mock_send_kafka_message.return_value = MagicMock()

    mock_verify.return_value = 2048

    mock_latency.return_value = 20

    img_file = b"bytes"
    num_bytes = 2048
    mock_create_img.return_value = img_file, num_bytes

    mock_uuid.return_value = "54321"

    mock_save_img.return_value = "ddm/'Venus'/54321.png"

    process_msg(msg, producer_topic, producer_config)

    mock_verify.assert_called_once_with( 
                    incoming_file=Path("/dsoc/incoming") / f"{mock_payload['transfer_uuid']}.bin",
                    expected_num_bytes=mock_payload["num_bytes"],
                    producer_topic=producer_topic,
                    producer_config=producer_config,
                    waveform_requester=str("user"),
                    gbt_event_time=str("2026-07-15T12:00:00+00:00"),
                    gbt_uuid=gbt_uuid,
                    object_id=str("fake_object_id"),
                    target=str("fake_target"),
                    tx_waveform=str("fake_tx_waveform"),
                    rec_waveform=str("fake_rec_waveform"),
                    filename=str("fake_filename.png"),
                    transfer_uuid=transfer_uuid,
                    vlba_station=Stations.PT,
                )      
    mock_latency.assert_called_once_with(datetime.fromisoformat("2026-07-15T12:00:00+00:00"), Stations.DSOC)
    mock_create_img.assert_called_once_with(Stations.PT, str("fake_tx_waveform"), waveform_requester=str("user"))
    mock_uuid.assert_called_once()
    mock_save_img.assert_called_once_with(
                    str("fake_target"),
                    b"bytes",
                    "54321",
                )
    mock_send_kafka_message.assert_has_calls([
        call(
            producer_topic=(producer_topic),
            producer_config=(producer_config),
            waveform_requester=str("user"),
            message_type=(Message.STATUS_UPDATE),
            transfer_uuid=(transfer_uuid),
            gbt_uuid=gbt_uuid,
            gbt_event_time=str("2026-07-15T12:00:00+00:00"),
            station=Stations.DSOC,
            status=Status.VERIFYING,
            object_id=str("fake_object_id"),
            target=str("fake_target"),
            tx_waveform=str("fake_tx_waveform"),
            rec_waveform=(str("fake_rec_waveform")),
            num_bytes=(2048),
            filename=str("fake_filename.png"),
            xmit_station=(Stations.GBT),
            rcvr_station=(Stations.PT),
            message=(
                    f"Verifying "
                    "fake_filename.png."
                ),
        ),
        call(
            producer_topic=(producer_topic),
            producer_config=(producer_config),
            waveform_requester=str("user"),
            message_type=(Message.VLBA_DELETE),
            transfer_uuid=(transfer_uuid),
            gbt_uuid=gbt_uuid,
            gbt_event_time=str("2026-07-15T12:00:00+00:00"),
            station=Stations.DSOC,
            status=Status.COMPLETED,
            object_id=str("fake_object_id"),
            target=str("fake_target"),
            tx_waveform=str("fake_tx_waveform"),
            rec_waveform=(str("fake_rec_waveform")),
            product_type="DDM",
            product_id=str("54321"),
            image_key="ddm/'Venus'/54321.png",
            num_bytes=(2048),
            filename=str("fake_filename.png"),
            latency_ms=(20),
            xmit_station=(Stations.GBT),
            rcvr_station=(Stations.PT),
            message=(
                f"DSOC verified {Stations.PT.label}'s "
                "e-transfer, generated "
                "the DDM image, stored "
                "the image, and completed "
                f"processing. {Stations.PT.label} may "
                "delete its raw data."
            ),
        ),
    ])
    assert mock_send_kafka_message.call_count == 2
#=====================================================================


"""Scenario 5: PROGRESS_COMPLETE incoming message. Verify incoming file FAILED case."""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
@patch("ngRadar_Website.management.commands.dsoc_sim.verify_incoming_transfer")
@patch("ngRadar_Website.management.commands.dsoc_sim.latency_calc")
@patch("ngRadar_Website.management.commands.dsoc_sim.create_img")
@patch("ngRadar_Website.management.commands.dsoc_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.dsoc_sim.save_image_to_seaweedfs")
def test_process_msg_PROGRESS_COMPLETE_verificationFAILED(
        mock_save_img,
        mock_uuid,
        mock_create_img,
        mock_latency,
        mock_verify,
        mock_json,
        mock_send_kafka_message,
        monkeypatch,
    ):
    
    monkeypatch.setenv("DSOC_VOLUME_SIZE", "2")

    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = str(
        Message.PROGRESS_COMPLETE.value
        ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": (transfer_uuid),
            "gbt_uuid": (gbt_uuid),
            "object_id": str("fake_object_id"),
            "target": str("fake_target"),
            "waveform_requester": str("user"),
            "tx_waveform": str("fake_tx_waveform"),
            "rec_waveform": str("fake_rec_waveform"),
            "gbt_event_time": str("2026-07-15T12:00:00+00:00"),
            "status": 1,
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "message": 2,
            "station": Stations.PT,
            "retry_count": 5,
        }

    mock_json.return_value = mock_payload

    mock_send_kafka_message.return_value = MagicMock()

    mock_verify.side_effect = RuntimeError

    process_msg(msg, producer_topic, producer_config)

    mock_verify.assert_called_once_with( 
                incoming_file=Path("/dsoc/incoming") / f"{mock_payload['transfer_uuid']}.bin",
                expected_num_bytes=mock_payload["num_bytes"],
                producer_topic=producer_topic,
                producer_config=producer_config,
                waveform_requester=str("user"),
                gbt_event_time=str("2026-07-15T12:00:00+00:00"),
                gbt_uuid=gbt_uuid,
                object_id=str("fake_object_id"),
                target=str("fake_target"),
                tx_waveform=str("fake_tx_waveform"),
                rec_waveform=str("fake_rec_waveform"),
                filename=str("fake_filename.png"),
                transfer_uuid=transfer_uuid,
                vlba_station=Stations.PT,
            )     
    assert mock_latency.call_count == 0
    assert mock_create_img.call_count == 0
    assert mock_save_img.call_count == 0
    assert mock_uuid.call_count == 0
    assert mock_send_kafka_message.call_count == 2
    mock_send_kafka_message.assert_has_calls([
            call(
                producer_topic=(producer_topic),
                producer_config=(producer_config),
                waveform_requester=str("user"),
                message_type=(Message.STATUS_UPDATE),
                transfer_uuid=(transfer_uuid),
                gbt_uuid=gbt_uuid,
                gbt_event_time=str("2026-07-15T12:00:00+00:00"),
                station=Stations.DSOC,
                status=Status.VERIFYING,
                object_id=str("fake_object_id"),
                target=str("fake_target"),
                tx_waveform=str("fake_tx_waveform"),
                rec_waveform=(str("fake_rec_waveform")),
                num_bytes=(2048),
                filename=str("fake_filename.png"),
                xmit_station=(Stations.GBT),
                rcvr_station=(Stations.PT),
                message=(
                        f"Verifying "
                        "fake_filename.png."
                    ),
            ),
            call(
                producer_topic=(producer_topic),
                producer_config=(producer_config),
                waveform_requester=str("user"),
                message_type=(Message.STATUS_UPDATE),
                transfer_uuid=(transfer_uuid),
                gbt_uuid=gbt_uuid,
                gbt_event_time=str("2026-07-15T12:00:00+00:00"),
                station=Stations.DSOC,
                status=Status.FAILED,
                object_id=str("fake_object_id"),
                target=str("fake_target"),
                tx_waveform=str("fake_tx_waveform"),
                rec_waveform=(str("fake_rec_waveform")),
                num_bytes=(0),
                filename=str("fake_filename.png"),
                xmit_station=(Stations.GBT),
                rcvr_station=(Stations.PT),
                message=(""),
            ),
        ])

#=====================================================================


"""Scenario 6: PROGRESS_COMPLETE incoming message. Image processing FAILED case. Arbitrarily picking save_to_seaweedfs to fail"""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
@patch("ngRadar_Website.management.commands.dsoc_sim.verify_incoming_transfer")
@patch("ngRadar_Website.management.commands.dsoc_sim.latency_calc")
@patch("ngRadar_Website.management.commands.dsoc_sim.create_img")
@patch("ngRadar_Website.management.commands.dsoc_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.dsoc_sim.save_image_to_seaweedfs")
def test_process_msg_PROGRESS_COMPLETE_processingFAILED(
        mock_save_img,
        mock_uuid,
        mock_create_img,
        mock_latency,
        mock_verify,
        mock_json,
        mock_send_kafka_message,
        monkeypatch,
    ):
    
    monkeypatch.setenv("DSOC_VOLUME_SIZE", "1")

    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = str(
        Message.PROGRESS_COMPLETE.value
        ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = {
            "transfer_uuid": (transfer_uuid),
            "gbt_uuid": (gbt_uuid),
            "object_id": str("fake_object_id"),
            "target": str("fake_target"),
            "waveform_requester": str("user"),
            "tx_waveform": str("fake_tx_waveform"),
            "rec_waveform": str("fake_rec_waveform"),
            "gbt_event_time": str("2026-07-15T12:00:00+00:00"),
            "status": 1,
            "num_bytes": 2048,
            "filename": str("fake_filename.png"),
            "message": 2,
            "station": Stations.PT,
            "retry_count": 5,
        }

    mock_json.return_value = mock_payload

    mock_send_kafka_message.return_value = MagicMock()

    mock_verify.return_value = 2048

    mock_latency.return_value = 20

    img_file = b"bytes"
    num_bytes = 2048
    mock_create_img.return_value = img_file, num_bytes

    mock_uuid.return_value = "54321"

    mock_save_img.side_effect = RuntimeError

    process_msg(msg, producer_topic, producer_config)

    mock_verify.assert_called_once_with( 
                    incoming_file=Path("/dsoc/incoming") / f"{mock_payload['transfer_uuid']}.bin",
                    expected_num_bytes=mock_payload["num_bytes"],
                    producer_topic=producer_topic,
                    producer_config=producer_config,
                    waveform_requester=str("user"),
                    gbt_event_time=str("2026-07-15T12:00:00+00:00"),
                    gbt_uuid=gbt_uuid,
                    object_id=str("fake_object_id"),
                    target=str("fake_target"),
                    tx_waveform=str("fake_tx_waveform"),
                    rec_waveform=str("fake_rec_waveform"),
                    filename=str("fake_filename.png"),
                    transfer_uuid=transfer_uuid,
                    vlba_station=Stations.PT,
                )      
    mock_latency.assert_called_once_with(datetime.fromisoformat("2026-07-15T12:00:00+00:00"), Stations.DSOC)
    mock_create_img.assert_called_once_with(Stations.PT, str("fake_tx_waveform"), waveform_requester=str("user"))
    mock_uuid.assert_called_once()
    mock_save_img.assert_called_once_with(
                    str("fake_target"),
                    b"bytes",
                    "54321",
                )
    mock_send_kafka_message.assert_has_calls([
        call(
            producer_topic=(producer_topic),
            producer_config=(producer_config),
            waveform_requester=str("user"),
            message_type=(Message.STATUS_UPDATE),
            transfer_uuid=(transfer_uuid),
            gbt_uuid=gbt_uuid,
            gbt_event_time=str("2026-07-15T12:00:00+00:00"),
            station=Stations.DSOC,
            status=Status.VERIFYING,
            object_id=str("fake_object_id"),
            target=str("fake_target"),
            tx_waveform=str("fake_tx_waveform"),
            rec_waveform=(str("fake_rec_waveform")),
            num_bytes=(2048),
            filename=str("fake_filename.png"),
            xmit_station=(Stations.GBT),
            rcvr_station=(Stations.PT),
            message=(
                    f"Verifying "
                    "fake_filename.png."
                ),
        ),
        call(
            producer_topic=(producer_topic),
            producer_config=(producer_config),
            waveform_requester=str("user"),
            message_type=(Message.STATUS_UPDATE),
            transfer_uuid=(transfer_uuid),
            gbt_uuid=gbt_uuid,
            gbt_event_time=str("2026-07-15T12:00:00+00:00"),
            station=Stations.DSOC,
            status=Status.FAILED,
            object_id=str("fake_object_id"),
            target=str("fake_target"),
            tx_waveform=str("fake_tx_waveform"),
            rec_waveform=(str("fake_rec_waveform")),
            num_bytes=(2048),
            filename=str("fake_filename.png"),
            xmit_station=(Stations.GBT),
            rcvr_station=(Stations.PT),
            message=(
                        "DSOC image processing "
                        "failed: "
                    ),
        ),
    ])
    assert mock_send_kafka_message.call_count == 2

#=====================================================================


"""Scenario 7: incoming message is not VLBA_REQUEST_STORAGE or PROGRESS_COMPLETE."""
#=====================================================================

@patch("ngRadar_Website.management.commands.dsoc_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.dsoc_sim.json.loads")
@patch("ngRadar_Website.management.commands.dsoc_sim.verify_incoming_transfer")
@patch("ngRadar_Website.management.commands.dsoc_sim.latency_calc")
@patch("ngRadar_Website.management.commands.dsoc_sim.create_img")
@patch("ngRadar_Website.management.commands.dsoc_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.dsoc_sim.save_image_to_seaweedfs")
def test_process_msg_invalid_key(
        mock_save_img,
        mock_uuid,
        mock_create_img,
        mock_latency,
        mock_verify,
        mock_json,
        mock_send_kafka_message,
        monkeypatch,
    ):
    
    monkeypatch.setenv("DSOC_VOLUME_SIZE", "1")

    #The fake kafka message in the correct format:
    msg = MagicMock()
    msg.value.return_value = b'{"message"}'
    msg.key.return_value = str(
        Message.DB_COMMITTED.value
        ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
    transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

    #The fake output of the json.loads() function:
    mock_payload = MagicMock()
    mock_json.return_value = mock_payload

    process_msg(msg, producer_topic, producer_config)

    assert mock_verify.call_count == 0
    assert mock_latency.call_count == 0
    assert mock_create_img.call_count == 0
    assert mock_uuid.call_count == 0
    assert mock_save_img.call_count == 0
    assert mock_send_kafka_message.call_count == 0