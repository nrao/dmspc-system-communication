from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from unittest.mock import ANY

import pytest
from ngRadar_Website.enums import Stations, Message
from uuid import uuid4

import ngRadar_Website.ui_kafka as ui_kafka
from ngRadar_Website.ui_kafka import (
    consume_ui_events,
    start_ui_kafka_consumer,
    _consumer_started,
    _consumer_lock,
    logger,
)

# ==============================================================================
# 1. consume_ui_events Test
# ==============================================================================

@patch("ngRadar_Website.ui_kafka.Consumer")
@patch("ngRadar_Website.ui_kafka.os.getenv")
def test_consume_ui_events(mock_getenv, mock_Consumer):

    mock_getenv.side_effect = lambda key, default=None: {
    "UI_KAFKA_TOPICS": "fake_topic1,fake_topic2",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "UI_KAFKA_GROUP_ID": "test-ui-group",
    }.get(key, default)

    mock_consumer = mock_Consumer.return_value

    mock_consumer.poll.side_effect = [
        None,
        RuntimeError("Stop test"),
    ]

    with pytest.raises(RuntimeError, match="Stop test"):
        consume_ui_events()

    mock_consumer.subscribe.assert_called_once()

# ==============================================================================
# 2. start_ui_kafka_consumer Test
# ==============================================================================

@patch("ngRadar_Website.ui_kafka.consume_ui_events")
@patch("ngRadar_Website.ui_kafka.threading.Thread")
def test_start_ui_kafka_consumer(mock_Thread, mock_consume_ui_events):
    ui_kafka._consumer_started = False

    mock_thread = mock_Thread.return_value

    start_ui_kafka_consumer()

    assert ui_kafka._consumer_started is True
    mock_Thread.assert_called_once_with(
        target=mock_consume_ui_events,
        name="ui-kafka-consumer",
        daemon=True,
    )