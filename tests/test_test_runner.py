import sys
import types
import unittest

sys.modules.setdefault("jsonpickle", types.SimpleNamespace(encode=lambda value: str(value)))
sys.modules.setdefault(
    "orjson",
    types.SimpleNamespace(
        OPT_APPEND_NEWLINE=0,
        OPT_INDENT_2=0,
        dumps=lambda value, option=0: str(value).encode("utf-8"),
    ),
)

from prosperity4bt.constants import LIMITS
from prosperity4bt.datamodel import Order, TradingState
from prosperity4bt.models.input import BacktestData, PriceRow
from prosperity4bt.test_runner import TestRunner


PRODUCT = "EMERALDS"


class FakeTrader:
    def run(self, state: TradingState):
        return {PRODUCT: [Order(PRODUCT, 100, 1)]}, 0, ""


class FixedOrdersTrader:
    def __init__(self, *orders: Order):
        self.orders = list(orders)

    def run(self, state: TradingState):
        return {PRODUCT: list(self.orders)}, 0, ""


class FakeDataReader:
    def read_from_file(self, round_num: int, day_num: int) -> BacktestData:
        return BacktestData(
            round_num=round_num,
            day_num=day_num,
            prices={
                0: {
                    PRODUCT: PriceRow(
                        day=day_num,
                        timestamp=0,
                        product=PRODUCT,
                        bid_prices=[80],
                        bid_volumes=[1],
                        ask_prices=[90],
                        ask_volumes=[LIMITS[PRODUCT]],
                        mid_price=100.0,
                        profit_loss=0.0,
                    )
                }
            },
            trades={0: {PRODUCT: []}},
            observations={},
            products=[PRODUCT],
            profit_loss={PRODUCT: 0.0},
        )


class TestRunnerTests(unittest.TestCase):
    def test_pnl_only_rows_are_recorded_after_matching(self):
        result = TestRunner(FakeTrader(), FakeDataReader(), 1, 0, pnl_only=True).run()

        self.assertEqual([row.pnl for row in result.pnl_rows], [10.0])

    def test_activity_logs_are_recorded_after_matching(self):
        result = TestRunner(FakeTrader(), FakeDataReader(), 1, 0, pnl_only=False).run()

        self.assertEqual([row.profit_loss for row in result.activity_logs], [10.0])

    def test_buy_orders_exceeding_remaining_capacity_are_rejected(self):
        trader = FixedOrdersTrader(Order(PRODUCT, 100, LIMITS[PRODUCT] + 1))
        result = TestRunner(trader, FakeDataReader(), 1, 0, pnl_only=False).run()

        self.assertEqual(result.activity_logs[0].profit_loss, 0.0)
        self.assertEqual(result.trades, [])
        self.assertIn("exceeded limit", result.sandbox_logs[0].sandbox_log)

    def test_buy_orders_at_remaining_capacity_are_allowed(self):
        trader = FixedOrdersTrader(Order(PRODUCT, 100, LIMITS[PRODUCT]))
        result = TestRunner(trader, FakeDataReader(), 1, 0, pnl_only=False).run()

        self.assertEqual(result.activity_logs[0].profit_loss, LIMITS[PRODUCT] * 10.0)
        self.assertEqual(len(result.trades), 1)


if __name__ == "__main__":
    unittest.main()
