from prosperity4bt.models.output import BacktestResult
import math

class SummaryPrinter:

    @staticmethod
    def print_day_summary(result: BacktestResult):
        final_activities = result.final_activities()
        product_lines = [f"{a.symbol}: {a.profit_loss:,.0f}" for a in final_activities]
        total_profit = sum(a.profit_loss for a in final_activities)

        timestamps = {a.timestamp for a in result.activity_logs}
        num_timesteps = len(timestamps)
        profit_per_timestep = total_profit / num_timesteps * 100 if num_timesteps else 0.0

        pnl_by_timestamp = {}
        for a in result.activity_logs:
            pnl_by_timestamp[a.timestamp] = pnl_by_timestamp.get(a.timestamp, 0.0) + a.profit_loss
        pnl_series = [pnl_by_timestamp[t] for t in sorted(pnl_by_timestamp)]
        returns = [pnl_series[i] - pnl_series[i - 1] for i in range(1, len(pnl_series))]
        std = math.sqrt(sum((x - sum(returns) / len(returns)) ** 2 for x in returns) / len(returns)) if returns else 0.0
        sharpe = ((sum(returns)/len(returns)) / std) * 100 if std else 0.0
        
        print(*reversed(product_lines), sep="\n")
        print(f"Profit per-ts: {profit_per_timestep:,.4f}")
        print(f"Sharpe: {sharpe:,.4f}")
        print(f"Total PnL: {total_profit:,.0f}")
        print()

    @staticmethod
    def print_overall_summary(results: list[BacktestResult]):
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
            pnl_series = [pnl_by_timestamp[t] for t in sorted(pnl_by_timestamp)]
            returns = [pnl_series[i] - pnl_series[i - 1] for i in range(1, len(pnl_series))]
            std = math.sqrt(sum((x - sum(returns) / len(returns)) ** 2 for x in returns) / len(returns)) if returns else 0.0
            sharpe = ((sum(returns)/len(returns)) / std) * 100 if std else 0.0

            print(
                f"Round {result.round_num} day {result.day_num}: "
                f"Profit per-ts: {profit_per_timestep:,.4f} | ",
                f"Sharpe: {sharpe:,.4f}",
                f"Total PnL: {profit:,.0f}"
            )

            total_profit += profit
            total_timesteps += num_timesteps
            all_returns.extend(returns)

        overall_profit_per_timestep = total_profit / total_timesteps * 100 if total_timesteps else 0.0
        overall_std = math.sqrt(sum((x - sum(all_returns) / len(all_returns)) ** 2 for x in all_returns) / len(all_returns)) if all_returns else 0.0
        overall_sharpe = ((sum(all_returns)/len(all_returns)) / overall_std) * 100 if overall_std else 0.0

        print("\nOverall summary:")
        print(f"Profit per-ts: {overall_profit_per_timestep:,.4f}")
        print(f"Sharpe: {overall_sharpe:,.4f}")
        print(f"Total PnL: {total_profit:,.0f}")
        print()