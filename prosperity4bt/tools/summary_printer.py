from prosperity4bt.models.output import BacktestResult
import numpy as np


class SummaryPrinter:

    @staticmethod
    def print_day_summary(result: BacktestResult):
        if result.is_pnl_only():
            SummaryPrinter.__print_pnl_only_summary(result)
            return

        final_activities = result.final_activities()
        product_lines = [f"{a.symbol}: {a.profit_loss:,.0f}" for a in final_activities]
        total_profit = sum(a.profit_loss for a in final_activities)

        timestamps = {a.timestamp for a in result.activity_logs}
        num_timesteps = len(timestamps)
        profit_per_timestep = total_profit / num_timesteps * 100 if num_timesteps else 0.0

        pnl_by_timestamp = {}
        for a in result.activity_logs:
            pnl_by_timestamp[a.timestamp] = pnl_by_timestamp.get(a.timestamp, 0.0) + a.profit_loss

        pnl_series = np.array([pnl_by_timestamp[t] for t in sorted(pnl_by_timestamp)], dtype=float)
        returns = np.diff(pnl_series)
        std = returns.std() if returns.size else 0.0
        sharpe = (returns.mean() / std) * 100 if std else 0.0

        print(*reversed(product_lines), sep="\n")
        print(f"Profit per-ts: {profit_per_timestep:,.4f}")
        print(f"Sharpe: {sharpe:,.4f}")
        print(f"Total PnL: {total_profit:,.0f}")
        print()

    @staticmethod
    def print_overall_summary(results: list[BacktestResult]):
        if all(result.is_pnl_only() for result in results):
            SummaryPrinter.__print_overall_pnl_only_summary(results)
            return

        print("Profit summary:")

        total_profit = 0
        total_timesteps = 0
        all_returns = []

        for result in results:
            final_activities = result.final_activities()
            profit = sum(a.profit_loss for a in final_activities)

            timestamps = {a.timestamp for a in result.activity_logs}
            num_timesteps = len(timestamps)
            profit_per_timestep = profit / num_timesteps * 100 if num_timesteps else 0.0

            pnl_by_timestamp = {}
            for a in result.activity_logs:
                pnl_by_timestamp[a.timestamp] = pnl_by_timestamp.get(a.timestamp, 0.0) + a.profit_loss

            pnl_series = np.array([pnl_by_timestamp[t] for t in sorted(pnl_by_timestamp)], dtype=float)
            returns = np.diff(pnl_series)
            std = returns.std() if returns.size else 0.0
            sharpe = (returns.mean() / std) * 100 if std else 0.0

            print(
                f"Round {result.round_num} day {result.day_num}: "
                f"Profit per-ts: {profit_per_timestep:,.4f} | ",
                f"Sharpe: {sharpe:,.4f}",
                f"Total PnL: {profit:,.0f}"
            )

            total_profit += profit
            total_timesteps += num_timesteps
            all_returns.extend(returns.tolist())

        all_returns = np.array(all_returns, dtype=float)
        overall_profit_per_timestep = total_profit / total_timesteps * 100 if total_timesteps else 0.0
        overall_std = all_returns.std() if all_returns.size else 0.0
        overall_sharpe = (all_returns.mean() / overall_std) * 100 if overall_std else 0.0

        print(f"\nProfit per-ts: {overall_profit_per_timestep:,.4f}")
        print(f"Sharpe: {overall_sharpe:,.4f}")
        print(f"Total PnL: {total_profit:,.0f}")

    @staticmethod
    def __print_pnl_only_summary(result: BacktestResult):
        pnl_series = np.array([row.pnl for row in result.pnl_rows], dtype=float)
        total_profit = pnl_series[-1] if pnl_series.size else 0.0
        num_timesteps = pnl_series.size
        profit_per_timestep = total_profit / num_timesteps * 100 if num_timesteps else 0.0
        returns = np.diff(pnl_series)
        std = returns.std() if returns.size else 0.0
        sharpe = (returns.mean() / std) * 100 if std else 0.0

        print(f"Profit per-ts: {profit_per_timestep:,.4f}")
        print(f"Sharpe: {sharpe:,.4f}")
        print(f"Total PnL: {total_profit:,.0f}")
        print()

    @staticmethod
    def __print_overall_pnl_only_summary(results: list[BacktestResult]):
        print("Profit summary:")

        total_profit = 0.0
        total_timesteps = 0
        all_returns = []

        for result in results:
            pnl_series = np.array([row.pnl for row in result.pnl_rows], dtype=float)
            profit = pnl_series[-1] if pnl_series.size else 0.0
            num_timesteps = pnl_series.size
            profit_per_timestep = profit / num_timesteps * 100 if num_timesteps else 0.0
            returns = np.diff(pnl_series)
            std = returns.std() if returns.size else 0.0
            sharpe = (returns.mean() / std) * 100 if std else 0.0

            print(
                f"Round {result.round_num} day {result.day_num}: "
                f"Profit per-ts: {profit_per_timestep:,.4f} | ",
                f"Sharpe: {sharpe:,.4f}",
                f"Total PnL: {profit:,.0f}"
            )

            total_profit += profit
            total_timesteps += num_timesteps
            all_returns.extend(returns.tolist())

        all_returns = np.array(all_returns, dtype=float)
        overall_profit_per_timestep = total_profit / total_timesteps * 100 if total_timesteps else 0.0
        overall_std = all_returns.std() if all_returns.size else 0.0
        overall_sharpe = (all_returns.mean() / overall_std) * 100 if overall_std else 0.0

        print(f"\nProfit per-ts: {overall_profit_per_timestep:,.4f}")
        print(f"Sharpe: {overall_sharpe:,.4f}")
        print(f"Total PnL: {total_profit:,.0f}")
