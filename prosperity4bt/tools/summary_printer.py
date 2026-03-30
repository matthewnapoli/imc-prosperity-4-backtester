from prosperity4bt.models.output import BacktestResult

class SummaryPrinter:

    @staticmethod
    def print_day_summary(result: BacktestResult):
        final_activities = result.final_activities()
        product_lines = [f"{a.symbol}: {a.profit_loss:,.0f}" for a in final_activities]
        total_profit = sum(a.profit_loss for a in final_activities)

        timestamps = {a.timestamp for a in result.activity_logs}
        num_timesteps = len(timestamps)
        profit_per_timestep = total_profit/num_timesteps*100 if num_timesteps else 0.0

        print(*reversed(product_lines), sep="\n")
        print(f"Profit per-ts: {profit_per_timestep:,.4f}")


    @staticmethod
    def print_overall_summary(results: list[BacktestResult]):
        print("Profit summary:")

        total_profit = 0
        total_timesteps = 0

        for result in results:
            final_activities = result.final_activities()
            profit = sum(a.profit_loss for a in final_activities)

            timestamps = {a.timestamp for a in result.activity_logs}
            num_timesteps = len(timestamps)
            profit_per_timestep = profit / num_timesteps * 100 if num_timesteps else 0.0

            print(
                f"Round {result.round_num} day {result.day_num}: "
                f"Profit per-ts: {profit_per_timestep:,.4f}"
            )

            total_profit += profit
            total_timesteps += num_timesteps

        overall_profit_per_timestep = total_profit / total_timesteps * 100 if total_timesteps else 0.0

        print(f"Profit per-ts: {overall_profit_per_timestep:,.4f}")
