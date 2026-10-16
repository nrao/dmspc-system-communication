from unittest.mock import patch, MagicMock, call
from ngRadar_Website.enums import Stations, Status, Message
from datetime import datetime, timezone
import uuid
from pathlib import Path
import subprocess
import json


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
    from ngRadar_Website.management.commands.vlba_sim import (
        process_msg,
        MAX_RESUME_ATTEMPTS,
    )


# ==============================================================================
# 1. process_msg Tests
# ==============================================================================

"""Scenario 1: GBT_TX incoming message. Clean run, no failure cases."""
#=====================================================================

@patch.dict(
    "os.environ",
    {
        "STATION_NAME": "PT",
    },
)
@patch("ngRadar_Website.management.commands.vlba_sim.create_file")
@patch("ngRadar_Website.management.commands.vlba_sim.Path")
@patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.vlba_sim.watch_for_file")
@patch("ngRadar_Website.management.commands.vlba_sim.Thread")
@patch("ngRadar_Website.management.commands.vlba_sim.uuid.uuid4")
def test_process_msg_GBT_TX(
        mock_uuid,
        mock_Thread,
        mock_watch_for_file,
        mock_send_kafka_message,
        mock_Path,
        mock_create
):
    msg = MagicMock()

    payload = {
        "gbt_uuid": "fake_uuid",
        "station": Stations.GBT,
        "event_time": "2026-09-21T12:00:00+00:00",
        "object_id": "30104",
        "target": "Moretus",
        "tx_waveform": "SineWave",
        "rec_waveform": "SineWave",
        "waveform_requester": "username",
    }

    msg.value.return_value = json.dumps(payload).encode("utf-8")
    
    msg.key.return_value = str(
        Message.GBT_TX.value
    ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    mock_uuid.return_value = "12345"

    # mock the thread so code coverage continues past this process
    mock_Thread.start.return_value = True

    # raw_data_path = Path("/raw_data")
    mock_raw_data_path = MagicMock()
    mock_Path.return_value = mock_raw_data_path

    # frame_path = raw_data_path / f"{transfer_uuid}.bin"
    mock_frame_path = MagicMock()
    mock_raw_data_path.__truediv__.return_value = mock_frame_path

    mock_frame_path.is_file.return_value = True
    mock_frame_path.stat.return_value.st_size = 500

    process_msg(msg, producer_topic, producer_config)

    assert mock_uuid.call_count == 1
    mock_Thread.assert_called_once_with(target=mock_create, args=(mock_frame_path,), daemon=True)
    mock_watch_for_file.assert_called_once_with(mock_frame_path)
    mock_send_kafka_message.assert_called_once_with(
        producer_topic=producer_topic,
        producer_config=producer_config,
        waveform_requester="username",
        message_type=(Message.VLBA_REQUEST_STORAGE),
        transfer_uuid="12345",
        gbt_uuid="fake_uuid",
        gbt_event_time="2026-09-21T12:00:00+00:00",
        station=Stations.PT,
        status=Status.QUEUED,
        object_id="30104",
        target="Moretus",
        tx_waveform="SineWave",
        rec_waveform="SineWave",
        num_bytes=500,
        filename=mock_frame_path.name,
        xmit_station=Stations.GBT,
        rcvr_station=Stations.PT,
        message=(
            "VLBA requested a storage "
            "check at DSOC."
        )
    )

#=====================================================================


"""Scenario 2: GBT_TX incoming message. Generated file does not exist FAILED case."""
#=====================================================================

@patch.dict(
    "os.environ",
    {
        "STATION_NAME": "PT",
    },
)
@patch("ngRadar_Website.management.commands.vlba_sim.create_file")
@patch("ngRadar_Website.management.commands.vlba_sim.Path")
@patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.vlba_sim.watch_for_file")
@patch("ngRadar_Website.management.commands.vlba_sim.Thread")
@patch("ngRadar_Website.management.commands.vlba_sim.uuid.uuid4")
def test_process_msg_GBT_TX_FAILED(
        mock_uuid,
        mock_Thread,
        mock_watch_for_file,
        mock_send_kafka_message,
        mock_Path,
        mock_create
):
    msg = MagicMock()

    payload = {
        "gbt_uuid": "fake_uuid",
        "station": Stations.GBT,
        "event_time": "2026-09-21T12:00:00+00:00",
        "object_id": "30104",
        "target": "Moretus",
        "tx_waveform": "SineWave",
        "rec_waveform": "SineWave",
        "waveform_requester": "username",
    }

    msg.value.return_value = json.dumps(payload).encode("utf-8")
    
    msg.key.return_value = str(
        Message.GBT_TX.value
    ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    mock_uuid.return_value = "12345"

    # mock the thread so code coverage continues past this process
    mock_Thread.start.return_value = True

    # raw_data_path = Path("/raw_data")
    mock_raw_data_path = MagicMock()
    mock_Path.return_value = mock_raw_data_path

    # frame_path = raw_data_path / f"{transfer_uuid}.bin"
    mock_frame_path = MagicMock()
    mock_raw_data_path.__truediv__.return_value = mock_frame_path

    mock_frame_path.is_file.return_value = False

    process_msg(msg, producer_topic, producer_config)

    assert mock_uuid.call_count == 1
    mock_Thread.assert_called_once_with(target=mock_create, args=(mock_frame_path,), daemon=True)
    mock_watch_for_file.assert_called_once_with(mock_frame_path)
    mock_send_kafka_message.assert_called_once_with(
        producer_topic=producer_topic,
        producer_config=producer_config,
        waveform_requester="username",
        message_type=(Message.VLBA_FAILED),
        transfer_uuid="12345",
        gbt_uuid="fake_uuid",
        gbt_event_time="2026-09-21T12:00:00+00:00",
        station=Stations.PT,
        status=Status.FAILED,
        object_id="30104",
        target="Moretus",
        tx_waveform="SineWave",
        rec_waveform="SineWave",
        num_bytes=0,
        filename=mock_frame_path.name,
        xmit_station=Stations.GBT,
        rcvr_station=Stations.PT,
        message=(
                    "VLBA source file "
                    "does not exist."
                ),
    )

#=====================================================================


"""Scenario 3: DSOC_RESPOND_STORAGE incoming message. Clean run, no failure cases."""
#=====================================================================

@patch.dict(
    "os.environ",
    {
        "STATION_NAME": "PT",
    },
)
@patch("ngRadar_Website.management.commands.vlba_sim.etc_send")
@patch("ngRadar_Website.management.commands.vlba_sim.create_file")
@patch("ngRadar_Website.management.commands.vlba_sim.Path")
@patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.vlba_sim.watch_for_file")
@patch("ngRadar_Website.management.commands.vlba_sim.Thread")
@patch("ngRadar_Website.management.commands.vlba_sim.uuid.uuid4")
def test_process_msg_DSOC_RESPOND_STORAGE(
        mock_uuid,
        mock_Thread,
        mock_watch_for_file,
        mock_send_kafka_message,
        mock_Path,
        mock_create,
        mock_etc_send,
):
    msg = MagicMock()

    payload = {
        "gbt_uuid": "fake_uuid",
        "transfer_uuid": "12345",
        "station": Stations.PT,
        "gbt_event_time": "2026-09-21T12:01:00+00:00",
        "object_id": "30104",
        "target": "Moretus",
        "tx_waveform": "SineWave",
        "rec_waveform": "SineWave",
        "filename": "fake_filename.png",
        "num_bytes": 500,
        "retry_count": 5,
        "status": Status.READY.value,
        "waveform_requester": "username",
    }

    msg.value.return_value = json.dumps(payload).encode("utf-8")
    
    msg.key.return_value = str(
        Message.DSOC_RESPOND_STORAGE.value
    ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    mock_uuid.return_value = "12345"

    # mock the thread so code coverage continues past this process
    mock_Thread.start.return_value = True

    # raw_data_path = Path("/raw_data")
    mock_raw_data_path = MagicMock()
    mock_Path.return_value = mock_raw_data_path

    # frame_path = raw_data_path / f"{transfer_uuid}.bin"
    mock_frame_path = MagicMock()
    mock_raw_data_path.__truediv__.return_value = mock_frame_path

    mock_frame_path.is_file.return_value = True
    mock_frame_path.stat.return_value.st_size = 500

    process_msg(msg, producer_topic, producer_config)

    assert mock_uuid.call_count == 0
    mock_Thread.assert_not_called()
    mock_watch_for_file.assert_not_called()
    mock_etc_send.assert_called_once_with(mock_frame_path)
    mock_send_kafka_message.assert_called_once_with(
        producer_topic="progress_tracking",
        producer_config=producer_config,
        waveform_requester="username",
        message_type=(Message.VLBA_TRANSFERRING),
        transfer_uuid="12345",
        gbt_uuid="fake_uuid",
        gbt_event_time="2026-09-21T12:01:00+00:00",
        station=Stations.PT,
        status=Status.TRANSFERRING,
        object_id="30104",
        target="Moretus",
        tx_waveform="SineWave",
        rec_waveform="SineWave",
        num_bytes=500,
        filename="fake_filename.png",
        xmit_station=Stations.GBT,
        rcvr_station=Stations.PT,
        message=(
                "Hancock VLBA has "
                "started sending the "
                "data file to DSOC "
                "via e-transfer."
            ),
    )

#=====================================================================


"""Scenario 4: DSOC_RESPOND_STORAGE incoming message. etc_send raises CalledProcessError exception FAILED case."""
#=====================================================================

@patch.dict(
    "os.environ",
    {
        "STATION_NAME": "PT",
    },
)
@patch("ngRadar_Website.management.commands.vlba_sim.wait_for_etd")
@patch("ngRadar_Website.management.commands.vlba_sim.etc_send")
@patch("ngRadar_Website.management.commands.vlba_sim.Path")
@patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.vlba_sim.watch_for_file")
@patch("ngRadar_Website.management.commands.vlba_sim.Thread")
@patch("ngRadar_Website.management.commands.vlba_sim.uuid.uuid4")
def test_process_msg_DSOC_RESPOND_STORAGE_CalledProcessError(
        mock_uuid,
        mock_Thread,
        mock_watch_for_file,
        mock_send_kafka_message,
        mock_Path,
        mock_etc_send,
        mock_wait_for_etd,
):
    msg = MagicMock()

    payload = {
        "gbt_uuid": "fake_uuid",
        "transfer_uuid": "12345",
        "station": Stations.PT,
        "gbt_event_time": "2026-09-21T12:01:00+00:00",
        "object_id": "30104",
        "target": "Moretus",
        "tx_waveform": "SineWave",
        "rec_waveform": "SineWave",
        "filename": "fake_filename.png",
        "num_bytes": 500,
        "retry_count": 5,
        "status": Status.READY.value,
    }

    msg.value.return_value = json.dumps(payload).encode("utf-8")
    
    msg.key.return_value = str(
        Message.DSOC_RESPOND_STORAGE.value
    ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    mock_uuid.return_value = "12345"

    # mock the thread so code coverage continues past this process
    mock_Thread.start.return_value = True

    # raw_data_path = Path("/raw_data")
    mock_raw_data_path = MagicMock()
    mock_Path.return_value = mock_raw_data_path

    # frame_path = raw_data_path / f"{transfer_uuid}.bin"
    mock_frame_path = MagicMock()
    mock_raw_data_path.__truediv__.return_value = mock_frame_path

    mock_frame_path.is_file.return_value = True
    mock_frame_path.stat.return_value.st_size = 500

    mock_etc_send.side_effect = subprocess.CalledProcessError(
            returncode=42,
            cmd="etc_send"
        )

    result = process_msg(msg, producer_topic, producer_config)

    assert result is False
    assert mock_uuid.call_count == 0
    mock_Thread.assert_not_called()
    mock_watch_for_file.assert_not_called()
    assert mock_etc_send.call_count == MAX_RESUME_ATTEMPTS
    assert mock_send_kafka_message.call_count == (MAX_RESUME_ATTEMPTS*2) #function is called twice every retry attempt

#=====================================================================


"""Scenario 5: DSOC_RESPOND_STORAGE incoming message. etc_send raises OSError exception FAILED case."""
#=====================================================================

@patch.dict(
    "os.environ",
    {
        "STATION_NAME": "PT",
    },
)
@patch("ngRadar_Website.management.commands.vlba_sim.wait_for_etd")
@patch("ngRadar_Website.management.commands.vlba_sim.etc_send")
@patch("ngRadar_Website.management.commands.vlba_sim.Path")
@patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.vlba_sim.watch_for_file")
@patch("ngRadar_Website.management.commands.vlba_sim.Thread")
@patch("ngRadar_Website.management.commands.vlba_sim.uuid.uuid4")
def test_process_msg_DSOC_RESPOND_STORAGE_OSError(
        mock_uuid,
        mock_Thread,
        mock_watch_for_file,
        mock_send_kafka_message,
        mock_Path,
        mock_etc_send,
        mock_wait_for_etd,
):
    msg = MagicMock()

    payload = {
        "gbt_uuid": "fake_uuid",
        "transfer_uuid": "12345",
        "station": Stations.PT,
        "gbt_event_time": "2026-09-21T12:01:00+00:00",
        "object_id": "30104",
        "target": "Moretus",
        "tx_waveform": "SineWave",
        "rec_waveform": "SineWave",
        "filename": "fake_filename.png",
        "num_bytes": 500,
        "retry_count": 5,
        "status": Status.READY.value,
    }

    msg.value.return_value = json.dumps(payload).encode("utf-8")
    
    msg.key.return_value = str(
        Message.DSOC_RESPOND_STORAGE.value
    ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    mock_uuid.return_value = "12345"

    # mock the thread so code coverage continues past this process
    mock_Thread.start.return_value = True

    # raw_data_path = Path("/raw_data")
    mock_raw_data_path = MagicMock()
    mock_Path.return_value = mock_raw_data_path

    # frame_path = raw_data_path / f"{transfer_uuid}.bin"
    mock_frame_path = MagicMock()
    mock_raw_data_path.__truediv__.return_value = mock_frame_path

    mock_frame_path.is_file.return_value = True
    mock_frame_path.stat.return_value.st_size = 500

    mock_etc_send.side_effect = OSError

    process_msg(msg, producer_topic, producer_config)

    assert mock_uuid.call_count == 0
    mock_Thread.assert_not_called()
    mock_watch_for_file.assert_not_called()
    assert mock_send_kafka_message.call_count == 2
    mock_etc_send.assert_called_once_with(mock_frame_path)

#=====================================================================


"""Scenario 6: DSOC_RESPOND_STORAGE incoming message. DSOC responded No, VLBA asks again."""
#=====================================================================

# @patch.dict(
#     "os.environ",
#     {
#         "STATION_NAME": "PT",
#     },
# )
# @patch("ngRadar_Website.management.commands.vlba_sim.etc_send")
# @patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
# @patch("ngRadar_Website.management.commands.vlba_sim.time.sleep")
# @patch("ngRadar_Website.management.commands.vlba_sim.record_transfer_event")
# @patch("ngRadar_Website.management.commands.vlba_sim.json.loads")
# def test_process_msg_DSOC_RESPOND_STORAGE_No(
#         mock_json,
#         mock_record_transfer_event,
#         mock_sleep,
#         mock_send_kafka_message,
#         mock_etc_send,
# ):
#     msg = MagicMock()
#     msg.value.return_value = b'{"value"}'
#     msg.key.return_value = b'2'

#     producer_topic = MagicMock()
#     producer_config = MagicMock()

#     #giving fake uuid's in the correct format so that 'uuid.UUID()' works on it in the function:
#     transfer_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
#     gbt_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")

#     #The fake output of the json.loads() function:
#     mock_payload = {
#         "transfer_uuid": str(transfer_uuid),
#         "gbt_uuid": str(gbt_uuid),
#         "status": 1,
#         "status_label": "READY",
#         "num_bytes": 2048,
#         "filename": str("fake_filename.png"),
#         "event_time": datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc),
#         "message": 1, # Same as message being "No"
#         "station": Stations.PT,
#     }

#     mock_json.return_value = mock_payload
#     mock_sleep.return_value = None # we don't want 5 seconds of sleep in test

#     process_msg(msg, producer_topic, producer_config)

#     assert mock_json.call_count == 1
#     assert mock_record_transfer_event.call_count == 0
#     mock_sleep.assert_called_once_with(5)
#     mock_send_kafka_message.assert_called_once_with(
#                     key = f"{Message.VLBA_REQUEST_STORAGE}",
#                     producer_topic=producer_topic,
#                     producer_config=producer_config,
#                     transfer_uuid="11111111-1111-1111-1111-111111111111",
#                     gbt_uuid="22222222-2222-2222-2222-222222222222",
#                     status=Status.READY,
#                     num_bytes=2048,
#                     filename="fake_filename.png",
#                     station=Stations.PT,
#                     message=1,
#                 )      
#     assert mock_etc_send.call_count == 0

#=====================================================================


"""Scenario 7: VLBA_DELETE incoming message. VLBA deletes raw data."""
#=====================================================================

@patch.dict(
    "os.environ",
    {
        "STATION_NAME": "PT",
    },
)
@patch("ngRadar_Website.management.commands.vlba_sim.delete_observation_data")
@patch("ngRadar_Website.management.commands.vlba_sim.Path")
@patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
def test_process_msg_VLBA_DELETE(
        mock_send_kafka_message,
        mock_Path,
        mock_delete,
):
    msg = MagicMock()

    payload = {
        "rcvr_station": Stations.PT,
        "filename": "fake_filename.png",
    }

    msg.value.return_value = json.dumps(payload).encode("utf-8")
    
    msg.key.return_value = str(
        Message.VLBA_DELETE.value
    ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    process_msg(msg, producer_topic, producer_config)

    mock_delete.assert_called_once_with("fake_filename.png")
    assert mock_send_kafka_message.call_count == 0

#=====================================================================


"""Scenario 8: Incoming message has invalid value."""
#=====================================================================

@patch.dict(
    "os.environ",
    {
        "STATION_NAME": "PT",
    },
)
@patch("ngRadar_Website.management.commands.vlba_sim.delete_observation_data")
@patch("ngRadar_Website.management.commands.vlba_sim.Path")
@patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
def test_process_msg_VLBA_invalid(
        mock_send_kafka_message,
        mock_Path,
        mock_delete,
        capsys,
):
    msg = MagicMock()
    msg.key.return_value = str(
        Message.UI_EVENT.value
    ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    process_msg(msg, producer_topic, producer_config)

    mock_delete.assert_not_called()
    assert mock_send_kafka_message.call_count == 0
    captured=capsys.readouterr()
    assert captured.out.strip() == "NOT A VALID KAFKA MESSAGE VALUE!"

#=====================================================================