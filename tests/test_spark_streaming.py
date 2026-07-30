import importlib.util
import json
from datetime import datetime
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    TimestampType,
)


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "crypto-dashboard"
    / "spark_streaming.py"
)
SPEC = importlib.util.spec_from_file_location("spark_streaming", MODULE_PATH)
spark_streaming = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(spark_streaming)


class RollingChangeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("RollingChangeTests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.schema = StructType(
            [
                StructField("price", DoubleType(), False),
                StructField("ts", TimestampType(), False),
            ]
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def create_prices(self, rows):
        return self.spark.createDataFrame(rows, self.schema)

    def get_window(self, changes, start, end):
        matches = [
            row
            for row in changes.collect()
            if row.window.start == start and row.window.end == end
        ]
        self.assertEqual(len(matches), 1)
        return matches[0]

    def test_window_rolls_across_wall_clock_minute_boundary(self):
        prices = self.create_prices(
            [
                (100.0, datetime.fromisoformat("2026-07-30T12:00:01")),
                (110.0, datetime.fromisoformat("2026-07-30T12:00:59")),
                (121.0, datetime.fromisoformat("2026-07-30T12:01:01")),
            ]
        )

        changes = spark_streaming.calculate_rolling_changes(prices)
        row = self.get_window(
            changes,
            datetime.fromisoformat("2026-07-30T12:00:02"),
            datetime.fromisoformat("2026-07-30T12:01:02"),
        )

        self.assertEqual(row.first_price, 110.0)
        self.assertEqual(row.last_price, 121.0)
        self.assertAlmostEqual(row.change, 0.1)
        self.assertEqual(
            spark_streaming.select_latest_change(changes),
            row,
        )

    def test_first_and_last_prices_follow_event_time_not_input_order(self):
        prices = self.create_prices(
            [
                (120.0, datetime.fromisoformat("2026-07-30T12:00:50")),
                (100.0, datetime.fromisoformat("2026-07-30T12:00:10")),
            ]
        )

        changes = spark_streaming.calculate_rolling_changes(prices)
        row = self.get_window(
            changes,
            datetime.fromisoformat("2026-07-30T12:00:00"),
            datetime.fromisoformat("2026-07-30T12:01:00"),
        )

        self.assertEqual(row.first_price, 100.0)
        self.assertEqual(row.last_price, 120.0)
        self.assertAlmostEqual(row.change, 0.2)

    def test_watermark_discards_windows_older_than_allowed_lateness(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            input_dir.mkdir()
            checkpoint_dir = root / "checkpoint"
            query_name = f"late_data_{uuid4().hex}"

            stream = (
                self.spark.readStream.schema(self.schema)
                .json(str(input_dir))
            )
            changes = spark_streaming.calculate_rolling_changes(stream)
            query = (
                changes.writeStream.format("memory")
                .queryName(query_name)
                .outputMode("update")
                .option("checkpointLocation", str(checkpoint_dir))
                .start()
            )
            try:
                (input_dir / "current.json").write_text(
                    "\n".join(
                        [
                            json.dumps(
                                {
                                    "price": 100.0,
                                    "ts": "2026-07-30T12:02:00",
                                }
                            ),
                            json.dumps(
                                {
                                    "price": 101.0,
                                    "ts": "2026-07-30T12:02:02",
                                }
                            ),
                        ]
                    ),
                    encoding="utf-8",
                )
                query.processAllAvailable()

                (input_dir / "late.json").write_text(
                    "\n".join(
                        [
                            json.dumps(
                                {
                                    "price": 50.0,
                                    "ts": "2026-07-30T12:00:00",
                                }
                            ),
                            json.dumps(
                                {
                                    "price": 51.0,
                                    "ts": "2026-07-30T12:00:02",
                                }
                            ),
                        ]
                    ),
                    encoding="utf-8",
                )
                query.processAllAvailable()

                late_rows = (
                    self.spark.table(query_name)
                    .filter("first_price = 50.0 OR last_price = 51.0")
                    .count()
                )
                self.assertEqual(late_rows, 0)
                self.assertGreaterEqual(
                    query.lastProgress["stateOperators"][0][
                        "numRowsDroppedByWatermark"
                    ],
                    1,
                )
            finally:
                query.stop()
                self.spark.catalog.dropTempView(query_name)

    def test_alert_threshold_is_strictly_greater_than_point_three_percent(self):
        base = {
            "first_price": 100.0,
            "last_price": 100.3,
            "last_ts": datetime.fromisoformat("2026-07-30T12:00:59"),
        }
        at_threshold = Row(change=0.003, **base)
        above_threshold = Row(change=0.0031, **base)

        self.assertIsNone(
            spark_streaming.build_alert_message(at_threshold)
        )
        alert = spark_streaming.build_alert_message(above_threshold)
        self.assertIsNotNone(alert)
        self.assertIn("上涨超0.3%", alert["alert"])


if __name__ == "__main__":
    unittest.main()
