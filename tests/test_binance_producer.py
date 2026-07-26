import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

import requests


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "crypto-dashboard"
    / "binance_producer.py"
)
SPEC = importlib.util.spec_from_file_location("binance_producer", MODULE_PATH)
binance_producer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(binance_producer)


class PublishLatestTradeTests(unittest.TestCase):
    def setUp(self):
        self.producer = Mock()

    def test_publishes_valid_trade(self):
        response = Mock()
        response.json.return_value = [{"price": "67234.50", "time": 1720000000123}]

        with patch.object(binance_producer.requests, "get", return_value=response) as get:
            result = binance_producer.publish_latest_trade(self.producer)

        self.assertTrue(result)
        get.assert_called_once_with(
            binance_producer.BINANCE_TRADES_URL,
            params=binance_producer.BINANCE_TRADES_PARAMS,
            timeout=binance_producer.REQUEST_TIMEOUT,
        )
        response.raise_for_status.assert_called_once_with()
        self.producer.send.assert_called_once()
        topic, payload = self.producer.send.call_args.args
        self.assertEqual(topic, "btc_price")
        self.assertEqual(
            json.loads(payload.decode("utf8")),
            {"price": 67234.5, "ts": 1720000000123},
        )

    def test_skips_invalid_responses(self):
        cases = [
            ("request timeout", requests.Timeout("timed out"), None),
            ("http error", requests.HTTPError("server error"), None),
            ("invalid json", None, ValueError("invalid json")),
            ("empty response", None, []),
            ("missing field", None, [{"price": "67234.50"}]),
            (
                "invalid numeric value",
                None,
                [{"price": "not-a-number", "time": 1720000000123}],
            ),
        ]

        for name, request_error, json_result in cases:
            with self.subTest(name=name):
                self.producer.reset_mock()
                response = Mock()
                if request_error:
                    response.raise_for_status.side_effect = request_error
                elif isinstance(json_result, Exception):
                    response.json.side_effect = json_result
                else:
                    response.json.return_value = json_result

                with patch.object(
                    binance_producer.requests,
                    "get",
                    return_value=response,
                ):
                    result = binance_producer.publish_latest_trade(self.producer)

                self.assertFalse(result)
                self.producer.send.assert_not_called()

    def test_recovers_on_the_next_poll_after_a_timeout(self):
        valid_response = Mock()
        valid_response.json.return_value = [
            {"price": "67234.50", "time": 1720000000123}
        ]

        with patch.object(
            binance_producer.requests,
            "get",
            side_effect=[requests.Timeout("timed out"), valid_response],
        ):
            first_result = binance_producer.publish_latest_trade(self.producer)
            second_result = binance_producer.publish_latest_trade(self.producer)

        self.assertFalse(first_result)
        self.assertTrue(second_result)
        self.producer.send.assert_called_once()


if __name__ == "__main__":
    unittest.main()
