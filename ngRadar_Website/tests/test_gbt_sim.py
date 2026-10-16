from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from unittest.mock import ANY
from ngRadar_Website.enums import Stations, Message
from uuid import uuid4


# =============================================
# TEST THE STANDALONE FUNCTIONS FROM GBT_SIM
# =============================================

# ==============================================================================
# IMPORTANT:
# Because we read "ngrok_endpoint.env" on import, we need to patch the Path globally
# before importing all the functions we want to test.
# ==============================================================================
mock_env_data = "BOOTSTRAP_SERVER=localhost:9092\nSOME_OTHER_VAR=value" 
with patch("pathlib.Path.read_text", return_value=mock_env_data):
    from ngRadar_Website.management.commands.gbt_sim import (
        process_msg,
    )

# ==============================================================================
# 1. process_msg Test
# ==============================================================================

@patch("ngRadar_Website.management.commands.gbt_sim.json.loads")
@patch("ngRadar_Website.management.commands.gbt_sim.uuid.uuid4")
@patch("ngRadar_Website.management.commands.gbt_sim.send_kafka_message")
@patch("ngRadar_Website.management.commands.gbt_sim.time.sleep")
def test_process_msg(mock_sleep, mock_kafka, mock_uuid, mock_json):

    UI_key = Message.UI_EVENT.value

    mock_msg = MagicMock()
    mock_msg.key.return_value = str(UI_key).encode("utf-8")

    mock_payload = {
        "tx_waveform": "99",
        "event_time": "2026-07-15T12:00:00+00:00",
        "waveform_requester": "user",
    }
    mock_json.return_value = mock_payload

    mock_uuid.return_value = MagicMock()

    mock_eventID = MagicMock()
    mock_kafka.return_value = mock_eventID

    producer_topic = "topic"
    producer_config = "config"

    result = process_msg(
        mock_msg,
        producer_topic,
        producer_config,
    )

    assert mock_json.call_count == 1
    assert mock_uuid.call_count == 1
    # assert mock_sleep.call_count == 1
    assert mock_kafka.call_count == 2
    assert result is True