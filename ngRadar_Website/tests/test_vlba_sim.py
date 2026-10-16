import os
os.environ["STATION_NAME"] = "PT"

from unittest.mock import patch, MagicMock, call
from ngRadar_Website.enums import Stations, Status, Message
from datetime import datetime, timezone
import uuid
from pathlib import Path
import subprocess
import json
from ngRadar_Website.management.commands.vlba_sim import (
    process_msg,
    MAX_RESUME_ATTEMPTS,
)

# =============================================
# TEST THE STANDALONE FUNCTIONS FROM DSOC_SIM
# =============================================

# ==============================================================================
# 1. process_msg Tests
# ==============================================================================

"""Scenario 1a: GBT_TX incoming message. Clean run, no failure cases. Streaming mode."""
#=====================================================================

@patch.dict(
    "os.environ",
    {
        "STATION_NAME": "PT",
        "EXPEDAT_MODE": "stream",
        "EXPEDAT_STREAM_MB": "10",
    },
)
@patch("ngRadar_Website.management.commands.vlba_sim.Path")
@patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.vlba_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.vlba_sim.tag_observation")
def test_process_msg_GBT_TX(
        mock_tag_obs,
        mock_uuid,
        mock_send_kafka_message,
        mock_Path,
):
    mock_tag_obs.return_value = None
    
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
    num_bytes = 10 * 1024 * 1024  # 10 MB

    mock_uuid.return_value = "12345"

    mock_raw_data_path = MagicMock()
    mock_Path.return_value = mock_raw_data_path

    mock_frame_path = MagicMock()
    mock_raw_data_path.__truediv__.return_value = mock_frame_path

    mock_frame_path.is_file.return_value = True

    process_msg(msg, producer_topic, producer_config)

    assert mock_uuid.call_count == 1
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
        num_bytes=num_bytes,
        filename=mock_frame_path.name,
        xmit_station=Stations.GBT,
        rcvr_station=Stations.PT,
        message=(
            "VLBA requested a storage "
            "check at DSOC."
        )
    )

#======================================================================
"""Scenario 1b: GBT_TX incoming message. Clean run, no failure cases. Transfer mode."""
#=====================================================================

@patch.dict(
    "os.environ",
    {
        "STATION_NAME": "PT",
        "EXPEDAT_MODE": "transfer",
        "EXPEDAT_STREAM_MB": "10",
    },
)
@patch("ngRadar_Website.management.commands.vlba_sim.Path")
@patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.vlba_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.vlba_sim.tag_observation")
@patch("ngRadar_Website.management.commands.vlba_sim.create_traced_file")
@patch("ngRadar_Website.management.commands.vlba_sim.watch_for_file")
def test_process_msg_GBT_TX(
        mock_watch_for_file,
        mock_create_traced_file,
        mock_tag_obs,
        mock_uuid,
        mock_send_kafka_message,
        mock_Path,
):
    mock_tag_obs.return_value = None
    
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
    num_bytes = 10 * 1024 * 1024  # 10 MB

    mock_uuid.return_value = "12345"

    mock_raw_data_path = MagicMock()
    mock_Path.return_value = mock_raw_data_path

    mock_frame_path = MagicMock()
    mock_raw_data_path.__truediv__.return_value = mock_frame_path

    mock_frame_path.is_file.return_value = True
    mock_frame_path.stat.return_value.st_size = 10 * 1024 * 1024  # 10 MB

    process_msg(msg, producer_topic, producer_config)

    assert mock_uuid.call_count == 1

    assert mock_create_traced_file.call_count == 1

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
        num_bytes=num_bytes,
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
        "EXPEDAT_MODE": "fake_mode",
    },
)
@patch("ngRadar_Website.management.commands.vlba_sim.Path")
@patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.vlba_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.vlba_sim.tag_observation")
def test_process_msg_GBT_TX_FAILED(
        mock_tag_obs,
        mock_uuid,
        mock_send_kafka_message,
        mock_Path,
):
    mock_tag_obs.return_value = None
    
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

    mock_raw_data_path = MagicMock()
    mock_Path.return_value = mock_raw_data_path

    mock_frame_path = MagicMock()
    mock_raw_data_path.__truediv__.return_value = mock_frame_path

    mock_frame_path.is_file.return_value = False

    process_msg(msg, producer_topic, producer_config)

    assert mock_uuid.call_count == 1
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
@patch("ngRadar_Website.management.commands.vlba_sim.Path")
@patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.vlba_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.vlba_sim.expedat_send")
@patch("ngRadar_Website.management.commands.vlba_sim.tag_observation")
def test_process_msg_DSOC_RESPOND_STORAGE(
        mock_tag_obs,
        mock_exp_send,
        mock_uuid,
        mock_send_kafka_message,
        mock_Path,
):
    mock_tag_obs.return_value = None
    
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
        "rcvr_station": Stations.PT,
    }

    msg.value.return_value = json.dumps(payload).encode("utf-8")
    
    msg.key.return_value = str(
        Message.DSOC_RESPOND_STORAGE.value
    ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    mock_uuid.return_value = "12345"

    mock_raw_data_path = MagicMock()
    mock_Path.return_value = mock_raw_data_path

    mock_frame_path = MagicMock()
    mock_raw_data_path.__truediv__.return_value = mock_frame_path

    mock_frame_path.is_file.return_value = True
    mock_frame_path.stat.return_value.st_size = 500

    process_msg(msg, producer_topic, producer_config)

    assert mock_uuid.call_count == 0
    mock_exp_send.assert_called_once()
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
                f"VLBA-{Stations.PT} has "
                "started sending the "
                "data file to DSOC "
                "via movedat."
            ),
    )

#=====================================================================


"""Scenario 4: DSOC_RESPOND_STORAGE incoming message. expedat_send raises CalledProcessError exception FAILED case."""
#=====================================================================

@patch.dict(
    "os.environ",
    {
        "STATION_NAME": "PT",
    },
)
@patch("ngRadar_Website.management.commands.vlba_sim.Path")
@patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.vlba_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.vlba_sim.expedat_send")
@patch("ngRadar_Website.management.commands.vlba_sim.wait_for_exp")
@patch("ngRadar_Website.management.commands.vlba_sim.tag_observation")
def test_process_msg_DSOC_RESPOND_STORAGE_CalledProcessError(
        mock_tag_obs,
        mock_wait_for_exp,
        mock_exp_send,
        mock_uuid,
        mock_send_kafka_message,
        mock_Path,
):  
    mock_tag_obs.return_value = None
    
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
        "rcvr_station": Stations.PT,
    }

    msg.value.return_value = json.dumps(payload).encode("utf-8")
    
    msg.key.return_value = str(
        Message.DSOC_RESPOND_STORAGE.value
    ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    mock_uuid.return_value = "12345"

    mock_raw_data_path = MagicMock()
    mock_Path.return_value = mock_raw_data_path

    mock_frame_path = MagicMock()
    mock_raw_data_path.__truediv__.return_value = mock_frame_path

    mock_frame_path.is_file.return_value = True
    mock_frame_path.stat.return_value.st_size = 500

    mock_exp_send.side_effect = subprocess.CalledProcessError(
            returncode=42,
            cmd="expedat_send"
        )

    mock_wait_for_exp.return_value = True

    result = process_msg(msg, producer_topic, producer_config)

    assert result is False
    assert mock_uuid.call_count == 0
    assert mock_send_kafka_message.call_count == (MAX_RESUME_ATTEMPTS*2) #function is called twice every retry attempt

#=====================================================================


"""Scenario 5: DSOC_RESPOND_STORAGE incoming message. expedat_send raises OSError exception FAILED case."""
#=====================================================================

@patch.dict(
    "os.environ",
    {
        "STATION_NAME": "PT",
    },
)
@patch("ngRadar_Website.management.commands.vlba_sim.Path")
@patch("ngRadar_Website.management.commands.vlba_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.vlba_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.vlba_sim.tag_observation")
def test_process_msg_DSOC_RESPOND_STORAGE_OSError(
        mock_tag_obs,
        mock_uuid,
        mock_send_kafka_message,
        mock_Path,
):
    mock_tag_obs.return_value = None

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
        "rcvr_station": Stations.PT,
    }

    msg.value.return_value = json.dumps(payload).encode("utf-8")
    
    msg.key.return_value = str(
        Message.DSOC_RESPOND_STORAGE.value
    ).encode("utf-8")

    producer_topic = MagicMock()
    producer_config = MagicMock()

    mock_uuid.return_value = "12345"

    mock_raw_data_path = MagicMock()
    mock_Path.return_value = mock_raw_data_path

    mock_frame_path = MagicMock()
    mock_raw_data_path.__truediv__.return_value = mock_frame_path

    mock_frame_path.is_file.return_value = True
    mock_frame_path.stat.return_value.st_size = 500

    process_msg(msg, producer_topic, producer_config)

    assert mock_uuid.call_count == 0
    assert mock_send_kafka_message.call_count == 2

#=====================================================================


"""Scenario 6: VLBA_DELETE incoming message. VLBA deletes raw data."""
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


"""Scenario 7: Incoming message has invalid value."""
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