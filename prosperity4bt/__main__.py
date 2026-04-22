import sys
from datetime import datetime
from typer import Argument, Context, Option, Typer
from typing import Annotated, Optional
from pathlib import Path
from prosperity4bt.back_tester import BackTester
from prosperity4bt.models.test_options import TestOptions, TradeMatchingMode, canonical_product

app = Typer(context_settings={"help_option_names": ["--help", "-h"], "allow_extra_args": True, "ignore_unknown_options": True})

@app.command()
def run(
    ctx: Context,
    algorithm: Annotated[Path, Argument(help="Path to the Python file containing the algorithm to backtest.", show_default=False,exists=True, file_okay=True, dir_okay=False, resolve_path=True)],
    round_num: Annotated[int, Argument(help="Round number to backtest. Optionally pass DAY and PRODUCT after it. DAY and PRODUCT default to 'all'.", show_default=False)],
    out: Annotated[Optional[Path], Option(help="File to save output log to (defaults to backtests/<timestamp>.log).", show_default=False, dir_okay=False, resolve_path=True)] = None,
    no_out: Annotated[bool, Option("--no-out", help="Skip saving output log.")] = False,
    data: Annotated[Optional[Path], Option(help="Path to a product-partitioned Parquet resources directory.", show_default=False, exists=True, file_okay=False, dir_okay=True, resolve_path=True)] = None,
    print_output: Annotated[bool, Option("--print", help="Print the trader's output to stdout while it's running.")] = False,
    match_trades: Annotated[TradeMatchingMode, Option(help="How to match orders against market trades. 'all' matches trades with prices equal to or worse than your quotes, 'worse' matches trades with prices worse than your quotes, 'none' does not match trades against orders at all.")] = TradeMatchingMode.worse,
    no_progress: Annotated[bool, Option("--no-progress", help="Don't show progress bars.")] = False,
    no_merge_pnl: Annotated[bool, Option("--no-merge-pnl", help="Merge profit and loss across days.")] = False,
    no_vis: Annotated[bool, Option("--no-vis", help="Open backtest results in https://matthewnapoli.github.io/imc-prosperity-4-visualizer/visualizer when done.")] = False,
    original_timestamps: Annotated[bool, Option("--original-timestamps", help="Preserve original timestamps in output log rather than making them increase across days.")] = False,
):
    if out is not None and no_out:
        print("Error: --out and --no-out are mutually exclusive")
        sys.exit(1)

    day, product = __parse_day_product(ctx.args)
    options = TestOptions(algorithm, round_num, day, product, __parse_out(out, no_out))
    options.back_data_dir = data
    options.print_output = print_output
    options.trade_matching_mode = match_trades
    options.show_progress = not no_progress
    options.merge_profit_loss = not no_merge_pnl
    options.show_visualizer = not no_vis
    options.merge_timestamps = not original_timestamps

    back_tester = BackTester(options)
    back_tester.run()


def __parse_day_product(args: list[str]) -> tuple[str, str]:
    if len(args) > 2:
        print("Error: expected at most two optional positional values: DAY and PRODUCT")
        sys.exit(1)

    for arg in args:
        if arg.startswith("--"):
            print(f"Error: unknown option {arg}")
            sys.exit(1)

    day = args[0].lower() if len(args) >= 1 else "all"
    product = __parse_product(args[1]) if len(args) == 2 else "all"

    return day, product


def __parse_product(product: str) -> str:
    return canonical_product(product)


def __parse_out(out: Optional[Path], no_out: bool) -> Optional[Path]:
    if out is not None:
        return out

    if no_out:
        return None

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return Path.cwd() / "backtests" / f"{timestamp}.log"


def main() -> None:
    app()

if __name__ == "__main__":
    main()
