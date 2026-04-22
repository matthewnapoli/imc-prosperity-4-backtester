import sys
import types
import unittest

sys.modules.setdefault("jsonpickle", types.SimpleNamespace(encode=lambda value: str(value)))

from prosperity4bt.tools.data_reader import ParquetResourcesReader


class FakeTable:
    def __init__(self, column_names):
        self.column_names = column_names


class ParquetResourcesReaderTests(unittest.TestCase):
    def test_fair_value_forward_fills_and_falls_back_to_mid_price(self):
        rows = [
            {"mid_price": 100.0, "fair_value": None},
            {"mid_price": 101.0, "fair_value": 200.0},
            {"mid_price": 102.0, "fair_value": None},
        ]

        fair_values = ParquetResourcesReader._ParquetResourcesReader__fair_values(
            FakeTable(["mid_price", "fair_value"]),
            rows,
        )

        self.assertEqual(fair_values, [100.0, 200.0, 200.0])

    def test_missing_fair_value_column_uses_mid_price(self):
        rows = [{"mid_price": 100.0}]

        fair_values = ParquetResourcesReader._ParquetResourcesReader__fair_values(
            FakeTable(["mid_price"]),
            rows,
        )

        self.assertEqual(fair_values, [100.0])


if __name__ == "__main__":
    unittest.main()
