from collections import defaultdict
from functools import reduce

from prosperity4bt.models.output import BacktestResult

# Merge a list of daily backtest results into one
class ResultMerger:

    def __init__(self, merge_timestamps: bool=True, merge_profit_loss: bool=False):
        # if merge_timestamps, the timestamps of the merged result will start with the last timestamp of the first result + 100, otherwise they start with be 0
        self.merge_timestamps = merge_timestamps

        # if merge_profit_loss, the profit/loss of the merged result will start with the profit/loss of the last activity log of the first result, otherwise they start with 0
        self.merge_profit_loss = merge_profit_loss


    def merge(self, results: list[BacktestResult]) -> BacktestResult:
        merged_result = reduce(lambda a, b: self.__merge_results(a, b), results)
        return merged_result


    def __merge_results(self, a: BacktestResult, b: BacktestResult) -> BacktestResult:
        if b.day_num <= a.last_day_num:
            raise ValueError(
                f"Cannot merge non-increasing day results: previous day {a.last_day_num}, next day {b.day_num}"
            )

        sandbox_logs = a.sandbox_logs[:]
        activity_logs = a.activity_logs[:]
        trades = a.trades[:]
        pnl_rows = a.pnl_rows[:]

        timestamp_offset = self.__timestamp_offset(a, b)
        sandbox_logs.extend([row.with_offset(timestamp_offset) for row in b.sandbox_logs])
        trades.extend([row.with_offset(timestamp_offset) for row in b.trades])

        profit_loss_offsets = self.__profile_loss_offset(a)
        activity_logs.extend([row.with_offset(timestamp_offset, profit_loss_offsets[row.symbol]) for row in b.activity_logs])
        pnl_offset = self.__pnl_offset(a)
        pnl_rows.extend([row.with_offset(timestamp_offset, pnl_offset) for row in b.pnl_rows])

        activity_value_column = a.activity_value_column
        if a.activity_value_column != b.activity_value_column:
            activity_value_column = "fair_value"

        result = BacktestResult(a.round_num, a.day_num, sandbox_logs, activity_logs, trades, activity_value_column, pnl_rows)
        result.last_day_num = b.last_day_num
        return result


    def __timestamp_offset(self, previous: BacktestResult, next_result: BacktestResult) -> int:
        if not self.merge_timestamps:
            return 0

        if len(previous.activity_logs) > 0:
            last_timestamp = previous.activity_logs[-1].timestamp
        elif len(previous.pnl_rows) > 0:
            last_timestamp = previous.pnl_rows[-1].timestamp
        else:
            last_timestamp = 0
        return last_timestamp + 100


    # return a dict of symbol -> profit/loss
    # if merge_profit_loss is False, return a dict of symbol -> 0
    # if merge_profit_loss is True, return a dict of symbol -> profit/loss of the last activity log of the previous result
    def __profile_loss_offset(self, previous: BacktestResult) -> dict[str, float]:
        profit_loss_offsets = defaultdict(float)
        if len(previous.activity_logs) == 0:
            return profit_loss_offsets

        last_timestamp = previous.activity_logs[-1].timestamp
        last_activities = [al for al in previous.activity_logs if al.timestamp == last_timestamp]
        for activity in last_activities:
            profit_loss_offsets[activity.symbol] = activity.profit_loss if self.merge_profit_loss else 0

        return profit_loss_offsets

    def __pnl_offset(self, previous: BacktestResult) -> float:
        if not self.merge_profit_loss or len(previous.pnl_rows) == 0:
            return 0.0

        return previous.pnl_rows[-1].pnl
