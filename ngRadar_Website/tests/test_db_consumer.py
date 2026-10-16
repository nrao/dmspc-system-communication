from unittest.mock import patch, MagicMock
from ngRadar_Website.models.models import ObservatoryEvent
from ngRadar_Website.enums import Message, Stations, UIEvent
import json

# ==============================================================================
# IMPORTANT:
# Because we read "ngrok_endpoint.env" on import, we need to patch the Path globally
# before importing all the functions we want to test.
# ==============================================================================
mock_env_data = "BOOTSTRAP_SERVER=localhost:9092\nSOME_OTHER_VAR=value" 
with patch("pathlib.Path.read_text", return_value=mock_env_data):
    from ngRadar_Website.management.commands.db_consumer import (
        process_msg,
        record_obs_event,
        publish_db_committed,
    )

# ==============================================================================
# 1. process_msg Test
# ==============================================================================

@patch("ngRadar_Website.management.commands.db_consumer.record_obs_event")
@patch("ngRadar_Website.management.commands.db_consumer.publish_db_committed")
@patch("ngRadar_Website.management.commands.db_consumer.transaction")
def test_process_msg_db_success(mock_transaction, mock_publish, mock_record):
    """Scenario 1: the 'try' is successful."""

    msg = MagicMock()

    payload = {
        "gbt_uuid": "fake_uuid",
        "transfer_uuid": "12345",
    }

    msg.value.return_value = (json.dumps(payload).encode("utf-8"))

    msg.key.return_value = str(Message.DSOC_RESPOND_STORAGE.value).encode("utf-8")

    msg.topic.return_value = "fake topic"

    mock_event = MagicMock()
    mock_event.uuid = ("11111111-1111-1111-1111-111111111111")
    mock_event.image_key = None
    mock_event.rcvr_station = None

    mock_record.return_value = (mock_event, True,)

    # Make `with transaction.atomic():` work.
    mock_transaction.atomic.return_value.__enter__.return_value = (None)

    # Execute on_commit callbacks immediately.
    mock_transaction.on_commit.side_effect = (lambda callback: callback())

    result = process_msg(
        msg,
        "fake topic",
        "fake config",
    )

    mock_record.assert_called_once_with(payload)

    mock_publish.assert_called_once_with(
        topic="fake topic",
        producer_config="fake config",
        payload=payload,
    )

    assert result is True


@patch("ngRadar_Website.management.commands.db_consumer.record_obs_event")
@patch("ngRadar_Website.management.commands.db_consumer.publish_db_committed")
@patch("ngRadar_Website.management.commands.db_consumer.transaction")
def test_process_msg_db_json_error(mock_transaction, mock_publish, mock_record, capsys):
    """Scenario 2: son.JSONDecodeError"""

    msg = MagicMock()
    msg.key.return_value = str(
            Message.DSOC_RESPOND_STORAGE.value
        ).encode("utf-8")
    
    payload = {
        "gbt_uuid": "fake_uuid",
        "transfer_uuid": "12345",
    }

    msg.value.return_value = b"invalid json message"

    result = process_msg(msg, "fake topic", "fake config")

    assert result == False
    captured = capsys.readouterr()
    assert "DB consumer received invalid JSON:" in captured.out

@patch("ngRadar_Website.management.commands.db_consumer.record_obs_event")
@patch("ngRadar_Website.management.commands.db_consumer.publish_db_committed")
@patch("ngRadar_Website.management.commands.db_consumer.transaction")
def test_process_msg_db_key_error(mock_transaction, mock_publish, mock_record, capsys):
    """Scenario 3: KeyError, TypeError, or ValueError (chose KeyError)"""

    msg = MagicMock()
    msg.key.return_value = str(
            "invalid key"
        ).encode("utf-8")
    
    payload = {
        "gbt_uuid": "fake_uuid",
        "transfer_uuid": "12345",
    }
    msg.value.return_value = json.dumps(payload).encode("utf-8")

    result = process_msg(msg, "fake topic", "fake config")

    assert result == False
    captured = capsys.readouterr()
    assert "DB consumer received invalid payload:" in captured.out

# ==============================================================================
# 2. record_obs_event Test
# ==============================================================================

@patch("ngRadar_Website.management.commands.db_consumer.ObservatoryEvent")
def test_record_obs_event(mock_ObservatoryEvent):

    mock_ObservatoryEvent.objects.update_or_create.return_value = ("obs_event", "created")

    mock_payload = {
                "transfer_uuid": "12345",
                "gbt_uuid": "67890",
                "object_id": str("fake_object_id"),
                "target": str("fake_target"),
                "tx_waveform": str("fake_tx_waveform"),
                "rec_waveform": str("fake_rec_waveform"),
                "event_time": str("2026-07-15T12:00:00+00:00"),
                "status": 1,
                "product_type": "DDM",
                "product_id": "fake_id",
                "xmit_station": Stations.GBT,
                "rcvr_station": Stations.PT,
                "image_key": "fake_key",
                "num_bytes": 2048,
                "latency_ms": 5,
                "message": 2,
                "station": Stations.PT,
                "event_uuid": "54321",
            }

    result1, result2 = record_obs_event(mock_payload)

    assert result1 == "obs_event"
    assert result2 == "created"
    mock_ObservatoryEvent.objects.update_or_create.assert_called_once()

# ==============================================================================
# 3. publish_db_committed Test
# ==============================================================================

@patch("ngRadar_Website.management.commands.db_consumer.produce")
def test_publish_db_committed(mock_produce):

    mock_produce.return_value = None

    publish_db_committed(topic="topic", producer_config="config", payload="payload")

    mock_produce.assert_called_once_with("topic",
                                         "config",
                                         str(Message.DB_COMMITTED.value),
                                         json.dumps({
                                                "event_type": "db_committed",
                                                "data": "payload",
                                            }))



#===================================================
# Test image_created sse produced from db_consumer
# to DSOC_notif
#===================================================
@patch("ngRadar_Website.management.commands.db_consumer." "publish_ui_event")
@patch("ngRadar_Website.management.commands.db_consumer." "record_obs_event")
@patch("ngRadar_Website.management.commands.db_consumer." "publish_db_committed")
@patch("ngRadar_Website.management.commands.db_consumer." "transaction")
def test_process_msg_publishes_image_changed(mock_transaction, mock_publish_db, mock_record, mock_publish_ui):
    msg = MagicMock()

    payload = {
        "event_uuid":
            "11111111-1111-1111-1111-111111111111",
        "image_key":
            "ddm/Moretus/image.png",
        "rcvr_station":
            Stations.PT,
    }

    msg.value.return_value = (json.dumps(payload).encode("utf-8"))

    msg.key.return_value = str(Message.DSOC_RESPOND_STORAGE.value).encode("utf-8")

    msg.topic.return_value = "DSOC_notif"

    mock_event = MagicMock()
    mock_event.uuid = payload["event_uuid"]
    mock_event.image_key = payload["image_key"]
    mock_event.rcvr_station = Stations.PT

    mock_record.return_value = (mock_event, True)

    mock_transaction.on_commit.side_effect = (lambda callback: callback())

    result = process_msg(
        msg,
        "DSOC_notif",
        "fake config",
    )

    mock_publish_ui.assert_called_once_with(
        topic="DSOC_notif",
        producer_config="fake config",
        event_type=UIEvent.IMAGE_CHANGED,
        key=payload["event_uuid"],
        data={
            "event_uuid": payload["event_uuid"],
            "rcvr_station": Stations.PT,
        },
    )

    assert result is True