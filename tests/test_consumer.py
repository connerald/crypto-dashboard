import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, call, patch

import socketio


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "crypto-dashboard"
    / "consumer.py"
)
SPEC = importlib.util.spec_from_file_location("consumer", MODULE_PATH)
consumer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(consumer)


class SocketIOConnectionTests(unittest.TestCase):
    def test_import_does_not_connect(self):
        self.assertFalse(consumer.sio.connected)

    def test_connects_without_retry_when_service_is_available(self):
        with (
            patch.object(consumer.sio, "connect") as connect,
            patch.object(consumer.time, "sleep") as sleep,
        ):
            consumer.connect_socketio()

        connect.assert_called_once_with(consumer.SOCKETIO_URL)
        sleep.assert_not_called()

    def test_retries_after_an_initial_connection_failure(self):
        error = socketio.exceptions.ConnectionError("service unavailable")

        with (
            patch.object(
                consumer.sio,
                "connect",
                side_effect=[error, None],
            ) as connect,
            patch.object(consumer.time, "sleep") as sleep,
        ):
            consumer.connect_socketio()

        self.assertEqual(
            connect.call_args_list,
            [
                call(consumer.SOCKETIO_URL),
                call(consumer.SOCKETIO_URL),
            ],
        )
        sleep.assert_called_once_with(
            consumer.SOCKETIO_RETRY_DELAY_SECONDS
        )

    def test_main_connects_before_starting_consumers_and_waiting(self):
        threads = [Mock(), Mock(), Mock()]

        with (
            patch.object(consumer, "connect_socketio") as connect,
            patch.object(
                consumer.threading,
                "Thread",
                side_effect=threads,
            ) as thread,
            patch.object(consumer.sio, "wait") as wait,
        ):
            consumer.main()

        connect.assert_called_once_with()
        self.assertEqual(
            thread.call_args_list,
            [
                call(target=consumer.alert_consumer, daemon=True),
                call(target=consumer.price_consumer, daemon=True),
                call(target=consumer.change_consumer, daemon=True),
            ],
        )
        for worker in threads:
            worker.start.assert_called_once_with()
        wait.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
