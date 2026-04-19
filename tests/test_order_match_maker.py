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

from prosperity4bt.datamodel import Observation, Order, OrderDepth, Trade, TradingState
from prosperity4bt.models.input import BacktestData
from prosperity4bt.models.test_options import TradeMatchingMode
from prosperity4bt.tools.order_match_maker import OrderMatchMaker


PRODUCT = "AMETHYSTS"
TIMESTAMP = 0


def build_state(order_depth: OrderDepth) -> TradingState:
    return TradingState(
        traderData="",
        timestamp=TIMESTAMP,
        listings={},
        order_depths={PRODUCT: order_depth},
        own_trades={},
        market_trades={},
        position={},
        observations=Observation({}, {}),
    )


def build_backtest_data(*trades: Trade) -> BacktestData:
    return BacktestData(
        round_num=0,
        day_num=0,
        prices={},
        trades={TIMESTAMP: {PRODUCT: list(trades)}},
        observations={},
        products=[PRODUCT],
        profit_loss={PRODUCT: 0},
    )


class OrderMatchMakerTests(unittest.TestCase):
    def test_worse_mode_blocks_buy_match_when_same_price_bid_exists(self):
        order_depth = OrderDepth()
        order_depth.buy_orders[101] = 5
        state = build_state(order_depth)
        back_data = build_backtest_data(Trade(PRODUCT, 100, 3, "buyer", "seller", TIMESTAMP))
        order = Order(PRODUCT, 101, 4)

        trades = OrderMatchMaker(
            state,
            back_data,
            {PRODUCT: [order]},
            TradeMatchingMode.worse,
        ).match()

        self.assertEqual(order.quantity, 4)
        self.assertNotIn(PRODUCT, state.own_trades)
        self.assertFalse(any(trade.trade_type == "make" for trade in trades))

    def test_worse_mode_still_allows_buy_match_without_same_price_bid(self):
        order_depth = OrderDepth()
        order_depth.buy_orders[100] = 5
        state = build_state(order_depth)
        back_data = build_backtest_data(Trade(PRODUCT, 99, 3, "buyer", "seller", TIMESTAMP))
        order = Order(PRODUCT, 101, 4)

        trades = OrderMatchMaker(
            state,
            back_data,
            {PRODUCT: [order]},
            TradeMatchingMode.worse,
        ).match()

        self.assertEqual(order.quantity, 1)
        self.assertEqual(state.position[PRODUCT], 3)
        self.assertTrue(any(trade.trade_type == "make" for trade in trades))

    def test_worse_mode_blocks_sell_match_when_same_price_ask_exists(self):
        order_depth = OrderDepth()
        order_depth.sell_orders[99] = -5
        state = build_state(order_depth)
        back_data = build_backtest_data(Trade(PRODUCT, 100, 3, "buyer", "seller", TIMESTAMP))
        order = Order(PRODUCT, 99, -4)

        trades = OrderMatchMaker(
            state,
            back_data,
            {PRODUCT: [order]},
            TradeMatchingMode.worse,
        ).match()

        self.assertEqual(order.quantity, -4)
        self.assertNotIn(PRODUCT, state.own_trades)
        self.assertFalse(any(trade.trade_type == "make" for trade in trades))


if __name__ == "__main__":
    unittest.main()
