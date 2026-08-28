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
        set_payload_dict,
        generate_payload,
        turn_off_transmitter,
        publish_gbtEvents,
        process_msg,
    )

# ==============================================================================
# 1. set_payload_dict Test
# ==============================================================================

#patching the function called within set_payload_dict to use a fake output:
@patch("ngRadar_Website.management.commands.gbt_sim.latency_calc")
def test_set_payload_dict(mock_latency):

    #fake variable setup:
    payload = {
        "object_id": None, 
        "target": None, 
        "tx_waveform": None, 
        "rec_waveform": None, 
        "event_time": None, 
        "latency_ms": None,
    }
    
    waveform = "sinewave"
    event_time = datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)

    mock_latency.return_value = 100

    #creating a range of times around event_time to have reasonable error bars:
    before = datetime.now(timezone.utc)
    payload = set_payload_dict(waveform, event_time)
    after = datetime.now(timezone.utc)

    #assert that the function correctly changed the payload values:
    assert payload["object_id"] == '30104'
    assert payload["target"] == 'Moretus'
    assert payload["tx_waveform"] == waveform
    assert payload["rec_waveform"] == waveform
    assert before <= payload["event_time"] <= after
    mock_latency.assert_called_once_with(event_time, Stations.GBT)
    assert payload["latency_ms"] == 100


# ==============================================================================
# 2. generate_payload Test
# ==============================================================================

# patch the database referenced, and the function called:
@patch("ngRadar_Website.management.commands.gbt_sim.uiEvent")
@patch("ngRadar_Website.management.commands.gbt_sim.set_payload_dict")
def test_generate_payload(mock_payload, mock_ui_event):
    # create fake values for the two variables referenced:
    mock_record = MagicMock()
    mock_record.selected_waveform = "SineWave"
    mock_record.event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    mock_ui_event.objects.get.return_value = mock_record
    
    # create a fake output to the set_payload_dict function:
    mock_payload.return_value = "payload"

    payload = generate_payload("uuid")

    mock_ui_event.objects.get.assert_called_once_with(uuid="uuid")
    mock_payload.assert_called_once_with("SineWave", datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert payload == "payload"


# ==============================================================================
# 3. turn_off_transmitter Test
# ==============================================================================

# patch the database referenced, and time.sleep:
@patch("ngRadar_Website.management.commands.gbt_sim.gbtEvent")
@patch("ngRadar_Website.management.commands.gbt_sim.time.sleep")
def test_turn_off_transmitter(mock_sleep, mock_gbt_event):
    # the patch has created the fake gbtEvent table, so we can call the function:
    turn_off_transmitter()

    mock_gbt_event.objects.create.assert_called_once_with(
        object_id="30104",
        target="Moretus",
        tx_waveform="Tx_OFF",
        rec_waveform="Tx_OFF",
        event_time=ANY,
        latency_ms=0,
    )
    mock_sleep.assert_called_once_with(5)


# ==============================================================================
# 4. publish_gbtEvents Test
# ==============================================================================

@patch("ngRadar_Website.management.commands.gbt_sim.gbtEvent")
def test_publish_gbtEvents(mock_gbt_event):
    input_data = {
            "object_id": 'fake_obj', 
            "target": 'fake_target', 
            "tx_waveform": 'Off', 
            "rec_waveform": 'Off', 
            "event_time": datetime(2026, 1, 1, tzinfo=timezone.utc), 
            "latency_ms": 0,
        }
    mock_instance = MagicMock()
    mock_instance.uuid = uuid4()
    mock_gbt_event.objects.create.return_value = mock_instance

    uuid = publish_gbtEvents(input_data)

    assert uuid == mock_instance.uuid
    mock_gbt_event.objects.create.assert_called_once_with(**input_data)


# ==============================================================================
# 5. process_msg Test
# ==============================================================================


@patch("ngRadar_Website.management.commands.gbt_sim.turn_off_transmitter")
@patch("ngRadar_Website.management.commands.gbt_sim.generate_payload")
@patch("ngRadar_Website.management.commands.gbt_sim.publish_gbtEvents")
@patch("ngRadar_Website.management.commands.gbt_sim.produce")
def test_process_msg(mock_produce, mock_publish_DB, mock_gen_payload, mock_transmitter):
    mock_msg = MagicMock()
    mock_msg.value.return_value = b"12345"

    producer_topic = "topic"
    producer_config = "config"

    payload = {
        "object_id": "fake_obj", 
        "target": "fake_target", 
        "tx_waveform": "Sinewave", 
        "rec_waveform": "Sinewave", 
        "event_time": datetime(2026, 1, 1, tzinfo=timezone.utc), 
        "latency_ms": 100,
    }
    mock_gen_payload.return_value = payload

    mock_publish_DB.return_value = "gbt_uuid"

    process_msg(mock_msg, producer_topic, producer_config)

    mock_transmitter.assert_called_once()
    mock_gen_payload.assert_called_once_with("12345")
    mock_publish_DB.assert_called_once_with(payload)
    mock_produce.assert_called_once_with("topic", "config", f"{Message.GBT_TX}", "gbt_uuid")