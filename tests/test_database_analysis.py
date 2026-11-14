import unittest

from aidbg.database_analysis import apply_baseline, history_series


class DatabaseAnalysisTests(unittest.TestCase):
    def test_high_spike_becomes_historical_anomaly(self):
        history = [
            {"signal": "connections", "value": 100 + (index % 3), "observed_at_unix_ms": index}
            for index in range(20)
        ]
        result = apply_baseline(
            {"signal": "connections", "value": 220, "severity": "info", "evidence": {}},
            history,
        )
        self.assertEqual(result["severity"], "critical")
        self.assertTrue(result["evidence"]["historical_anomaly"])
        self.assertEqual(result["evidence"]["baseline_samples"], 20)

    def test_low_free_storage_is_an_anomaly(self):
        history = [
            {"signal": "free_storage_bytes", "value": 1000, "observed_at_unix_ms": index}
            for index in range(12)
        ]
        result = apply_baseline(
            {"signal": "free_storage_bytes", "value": 100, "severity": "info", "evidence": {}},
            history,
        )
        self.assertEqual(result["severity"], "critical")

    def test_history_series_is_chronological(self):
        signals = [
            {
                "source": "orders",
                "signal": "cpu",
                "unit": "percent",
                "value": 30,
                "observed_at_unix_ms": 3,
                "evidence": {"historical_anomaly": True},
            },
            {
                "source": "orders",
                "signal": "cpu",
                "unit": "percent",
                "value": 20,
                "observed_at_unix_ms": 2,
            },
        ]
        series = history_series(signals)
        self.assertEqual([point["value"] for point in series[0]["points"]], [20.0, 30.0])
        self.assertTrue(series[0]["anomaly"])


if __name__ == "__main__":
    unittest.main()
