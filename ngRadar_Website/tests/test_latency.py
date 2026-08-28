import json
from datetime import datetime
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from ngRadar_Website.views.views import get_Message_Latency
from ngRadar_Website.enums import Stations


class GetMessageLatencyTests(SimpleTestCase):

    @patch("ngRadar_Website.views.views.ObservatoryEvent.objects")
    def test_get_message_latency(self, mock_objects):
        event_1 = MagicMock()
        event_1.latency_ms = 12.34567
        event_1.station = Stations.HN
        event_1.status = 1
        event_1.event_time = datetime(2026, 8, 14, 10, 30, 0)
        event_1.object_id = 30104
        event_1.target = "Moretus"
        event_1.get_station_display.return_value = "Hancock"
        event_1.get_status_display.return_value = "Completed"

        event_2 = MagicMock()
        event_2.latency_ms = 98.76543
        event_2.station = Stations.DSOC
        event_2.status = 1
        event_2.event_time = datetime(2026, 8, 14, 10, 31, 0)
        event_2.object_id = 30105
        event_2.target = "W48"
        event_2.get_station_display.return_value = (
            "Domenici Socorro Operations Center"
        )
        event_2.get_status_display.return_value = "Completed"

        # The DB query orders newest -> oldest.
        mock_queryset = MagicMock()
        mock_objects.exclude.return_value = mock_queryset
        mock_queryset.order_by.return_value.__getitem__.return_value = [
            event_2,
            event_1,
        ]

        # Act
        result = next(get_Message_Latency())

        # Assert ORM calls
        mock_objects.exclude.assert_called_once_with(
            tx_waveform="Tx_OFF"
        )
        mock_queryset.order_by.assert_called_once_with("-event_time")

        # Remove SSE prefix/suffix and parse JSON.
        payload = json.loads(
            result.removeprefix("data: ").strip()
        )

        self.assertEqual(
            payload["latency_array"],
            [12.346, 98.765],
        )

        self.assertEqual(
            payload["event_source_array"],
            ["HN", "DSOC"],
        )

        self.assertEqual(
            payload["event_metadata_array"],
            [
                {
                    "station": "Hancock",
                    "status": "Completed",
                    "time": "2026-08-14 10:30:00",
                    "object_id": 30104,
                    "target": "Moretus",
                },
                {
                    "station": "Domenici Socorro Operations Center",
                    "status": "Completed",
                    "time": "2026-08-14 10:31:00",
                    "object_id": 30105,
                    "target": "W48",
                },
            ],
        )


    @patch("ngRadar_Website.views.views.ObservatoryEvent.objects")
    def test_get_message_latency_handles_missing_values(self, mock_objects):
        event = MagicMock()
        event.latency_ms = 10.0
        event.station = None
        event.status = None
        event.event_time = datetime(2026, 8, 14, 10, 30, 0)
        event.object_id = None
        event.target = None

        mock_queryset = MagicMock()
        mock_objects.exclude.return_value = mock_queryset
        mock_queryset.order_by.return_value.__getitem__.return_value = [event]

        result = next(get_Message_Latency())

        payload = json.loads(
            result.removeprefix("data: ").strip()
        )

        self.assertEqual(payload["event_source_array"], ["Unknown"])

        self.assertEqual(
            payload["event_metadata_array"][0],
            {
                "station": "Unknown",
                "status": "-",
                "time": "2026-08-14 10:30:00",
                "object_id": "-",
                "target": "-",
            },
        )