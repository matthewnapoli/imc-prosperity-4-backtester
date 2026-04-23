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
    def test_configure_algorithm_mode_uses_algorithm_default_when_no_cli_override(self):
        options = TestOptions(Path("algo.py"), 0, "all", "all", None)
        back_tester = BackTester(options)

        trader_module = types.SimpleNamespace(
            Trader=FakeTrader,
            TRADER_MODE="submission_mode",
            VALID_TRADER_MODES={"submission_mode", "backtest_mode", "grid_search_mode"},
        )
        back_tester._BackTester__configure_algorithm_mode(trader_module)

        self.assertEqual(trader_module.TRADER_MODE, "submission_mode")

    def test_configure_algorithm_mode_cli_override_uses_algorithm_constants(self):
        options = TestOptions(Path("algo.py"), 0, "all", "all", None)
        options.run_mode = RunMode.gs
        back_tester = BackTester(options)

        trader_module = types.SimpleNamespace(
            Trader=FakeTrader,
            TRADER_MODE="submission_mode",
            SUBMISSION_MODE="submission_mode",
            BACKTEST_MODE="backtest_mode",
            GRID_SEARCH_MODE="grid_search_mode",
            VALID_TRADER_MODES={"submission_mode", "backtest_mode", "grid_search_mode"},
        )
        back_tester._BackTester__configure_algorithm_mode(trader_module)

        self.assertEqual(trader_module.TRADER_MODE, "grid_search_mode")

    def test_configure_algorithm_mode_prefers_setter_when_present(self):
        options = TestOptions(Path("algo.py"), 0, "all", "all", None)
        options.run_mode = RunMode.bt
        back_tester = BackTester(options)

        trader_module = types.SimpleNamespace(
            Trader=FakeTrader,
            TRADER_MODE="submission_mode",
            BACKTEST_MODE="backtest_mode",
            VALID_TRADER_MODES={"submission_mode", "backtest_mode"},
        )
        trader_module.set_calls = []

        def set_trader_mode(mode: str) -> None:
            trader_module.set_calls.append(mode)
            trader_module.TRADER_MODE = mode

        trader_module.set_trader_mode = set_trader_mode

        back_tester._BackTester__configure_algorithm_mode(trader_module)

        self.assertEqual(trader_module.set_calls, ["backtest_mode"])
        self.assertEqual(trader_module.TRADER_MODE, "backtest_mode")

    def test_configure_algorithm_mode_requires_trader_mode(self):
        options = TestOptions(Path("algo.py"), 0, "all", "all", None)
        back_tester = BackTester(options)
        trader_module = types.SimpleNamespace(Trader=FakeTrader)

        with self.assertRaises(SystemExit):
            back_tester._BackTester__configure_algorithm_mode(trader_module)


if __name__ == "__main__":
    unittest.main()
