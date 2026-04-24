import sys
import types
import unittest

sys.modules.setdefault(
    "orjson",
    types.SimpleNamespace(
        OPT_APPEND_NEWLINE=0,
        OPT_INDENT_2=0,
        dumps=lambda value, option=0: str(value).encode("utf-8"),
    ),
)

from prosperity4bt.models.output import BacktestResult, PnlRow
from prosperity4bt.tools.result_merger import ResultMerger


class ResultMergerTests(unittest.TestCase):
    def test_merge_pnl_only_results_offsets_timestamps(self):
        first = BacktestResult(1, 0, pnl_rows=[PnlRow(1, 0, 0, 10.0), PnlRow(1, 0, 100, 12.0)])
        second = BacktestResult(1, 1, pnl_rows=[PnlRow(1, 1, 0, 3.0), PnlRow(1, 1, 100, 4.0)])

        merged = ResultMerger(merge_timestamps=True, merge_profit_loss=False).merge([first, second])

        self.assertEqual([row.timestamp for row in merged.pnl_rows], [0, 100, 200, 300])
        self.assertEqual([row.pnl for row in merged.pnl_rows], [10.0, 12.0, 3.0, 4.0])

    def test_merge_pnl_only_results_can_carry_forward_pnl(self):
        first = BacktestResult(1, 0, pnl_rows=[PnlRow(1, 0, 0, 10.0), PnlRow(1, 0, 100, 12.0)])
        second = BacktestResult(1, 1, pnl_rows=[PnlRow(1, 1, 0, 3.0), PnlRow(1, 1, 100, 4.0)])

        merged = ResultMerger(merge_timestamps=True, merge_profit_loss=True).merge([first, second])

        self.assertEqual([row.pnl for row in merged.pnl_rows], [10.0, 12.0, 15.0, 16.0])


if __name__ == "__main__":
    unittest.main()
