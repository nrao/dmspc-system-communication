from datetime import datetime, timedelta, timezone
import pytest
from unittest.mock import patch, MagicMock
from ngRadar_Website.enums import Stations


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

@patch("ngRadar_Website.utils.AdminClient")
def test_config_func_GBT(mock_AdminClient):
    """Scenario 1: sim is GBT"""

    #dealing with the create_topic function, which calls f.result:
    future = MagicMock()
    future.result.return_value = None
    
    mock_admin = mock_AdminClient.return_value
    mock_admin.create_topics.return_value = {
        "user_input": future,
        "GBT_data": future,
    }

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
    mock_AdminClient.assert_called_once_with(
        {"bootstrap.servers": bootstrap}
    )


@pytest.mark.parametrize("sim", [
        (Stations.SC),
        (Stations.HN),
        (Stations.FD)
    ])
@patch("ngRadar_Website.utils.AdminClient")
def test_config_func_vlba(mock_AdminClient, sim):
    """Scenario 2: sim is a VLBA site"""

    #dealing with the create_topic function, which calls f.result:
    future = MagicMock()
    future.result.return_value = None
    
    mock_admin = mock_AdminClient.return_value
    mock_admin.create_topics.return_value = {
        "GBT_data": future,
        "VLBA_data": future,
    }

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
    mock_AdminClient.assert_called_once_with(
        {"bootstrap.servers": bootstrap}
    )

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

@patch("ngRadar_Website.utils.Path.read_text")
@patch("ngRadar_Website.utils.config_func")
def test_bootstrap_GBT(mock_config_func, mock_path):
    """Scenario 1: sim = GBT"""

    sim = Stations.GBT

    mock_env_data = "BOOTSTRAP_SERVER=localhost:9092\nSOME_OTHER_VAR=value"
    mock_path.return_value = mock_env_data
    
    mock_config_func.return_value = (
        "GBT_data",
        {"bootstrap.servers": "localhost:9092"},
        ["user_input"],
        {"bootstrap.servers": "localhost:9092"},
    )

    producer_topic, producer_config, consumer_topic, consumer_config = bootstrap(sim)

    mock_config_func.assert_called_once_with(sim, "localhost:9092")
    assert producer_topic == "GBT_data"
    assert producer_config == {"bootstrap.servers": "localhost:9092"}
    assert consumer_topic == ["user_input"]
    assert consumer_config == {"bootstrap.servers": "localhost:9092"}


@patch("ngRadar_Website.utils.Path.read_text")
@patch("ngRadar_Website.utils.config_func")
def test_bootstrap_DSOC(mock_config_func, mock_path):
    """Scenario 2: sim = DSOC"""

    sim = Stations.DSOC

    mock_env_data = "BOOTSTRAP_SERVER=localhost:9092\nSOME_OTHER_VAR=value"
    mock_path.return_value = mock_env_data
    
    mock_config_func.return_value = (
        "GBT_data",
        {"bootstrap.servers": "localhost:9092"},
    )

    topic, config = bootstrap(sim)

    mock_config_func.assert_called_once_with(sim, "localhost:9092")
    assert topic == "GBT_data"
    assert config == {"bootstrap.servers": "localhost:9092"}


@pytest.mark.parametrize("sim", [
        (Stations.GBT),
        (Stations.DSOC)
    ])
@patch("ngRadar_Website.utils.Path.read_text")
def test_bootstrap_none(mock_path, sim):
    """Scenario 3: bootstrap not found"""

    mock_env_data = "BOOTSTRAP_SERVER=\nSOME_OTHER_VAR=value"
    mock_path.return_value = mock_env_data

    with pytest.raises(
        RuntimeError,
        match="BOOTSTRAP_SERVER not found in /out/ngrok_endpoint.env",
    ):
        bootstrap(sim)



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