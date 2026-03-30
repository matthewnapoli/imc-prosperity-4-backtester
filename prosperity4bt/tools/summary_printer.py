from prosperity4bt.models.output import BacktestResult

class SummaryPrinter:

    @staticmethod
    def print_day_summary(result: BacktestResult):
        final_activities = result.final_activities()
        product_lines = [f"{a.symbol}: {a.profit_loss:,.0f}" for a in final_activities]
        total_profit = sum(a.profit_loss for a in final_activities)

        timestamps = {a.timestamp for a in result.activity_logs}
        num_timesteps = len(timestamps)
        profit_per_timestep = total_profit / num_timesteps if num_timesteps else 0.0

        print(*reversed(product_lines), sep="\n")
        print(f"Total profit: {total_profit:,.0f}")
        print(f"Profit per timestep: {profit_per_timestep:,.4f}")


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
            profit_per_timestep = profit / num_timesteps if num_timesteps else 0.0

            print(
                f"Round {result.round_num} day {result.day_num}: "
                f"{profit:,.0f} "
                f"({profit_per_timestep:,.4f} per timestep)"
            )

            total_profit += profit
            total_timesteps += num_timesteps

        overall_profit_per_timestep = total_profit / total_timesteps if total_timesteps else 0.0

        print(f"Total profit: {total_profit:,.0f}")
        print(f"Overall profit per timestep: {overall_profit_per_timestep:,.4f}")
