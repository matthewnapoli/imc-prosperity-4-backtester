import sys
import types
import unittest
from pathlib import Path

sys.modules.setdefault("jsonpickle", types.SimpleNamespace(encode=lambda value: str(value)))
sys.modules.setdefault(
    "orjson",
    types.SimpleNamespace(
        OPT_APPEND_NEWLINE=0,
        OPT_INDENT_2=0,
        dumps=lambda value, option=0: str(value).encode("utf-8"),
    ),
)

from prosperity4bt.back_tester import BackTester
from prosperity4bt.models.test_options import RunMode, TestOptions


class FakeTrader:
    pass


class RunModeTests(unittest.TestCase):
    def test_configure_algorithm_mode_sets_module_and_trader_flags(self):
        options = TestOptions(Path("algo.py"), 0, "all", "all", None)
        options.run_mode = RunMode.submission
        back_tester = BackTester(options)

        trader_module = types.SimpleNamespace(Trader=FakeTrader)
        trader = FakeTrader()
        back_tester._BackTester__configure_algorithm_mode(trader_module, trader)

        self.assertEqual(trader_module.BT_MODE, "submission")
        self.assertFalse(trader_module.IS_BACKTEST)
        self.assertTrue(trader_module.IS_SUBMISSION)
        self.assertFalse(trader_module.IS_GRID_SEARCH)
        self.assertEqual(trader.mode, "submission")
        self.assertFalse(trader.bt)
        self.assertTrue(trader.submission)
        self.assertFalse(trader.gs)
        self.assertFalse(trader.grid_search)
        self.assertFalse(trader.is_backtest)
        self.assertTrue(trader.is_submission)
        self.assertFalse(trader.is_grid_search)


if __name__ == "__main__":
    unittest.main()
