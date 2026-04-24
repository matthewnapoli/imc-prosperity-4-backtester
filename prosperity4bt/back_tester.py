import sys
from importlib import import_module, reload
from pathlib import Path
from typing import Any
from prosperity4bt.tools.data_reader import BackDataReader, ParquetResourcesReader
from prosperity4bt.models.output import BacktestResult
from prosperity4bt.tools.output_file_writer import OutputFileWriter
from prosperity4bt.tools.result_merger import ResultMerger
from prosperity4bt.tools.summary_printer import SummaryPrinter
from prosperity4bt.models.test_options import RunMode, TestOptions
from prosperity4bt.test_runner import TestRunner
from prosperity4bt.tools.visualizer import Visualizer


class BackTester:
    def __init__(self, options: TestOptions):
        self.options = options

    def run(self):
        print(f"running algorithm '{self.options.algorithm_path}'...")

        trader_module = self.__load_algorithm()
        data_reader = self.__get_data_reader()
        days = self.__resolve_days(data_reader)
        products = self.__resolve_products(data_reader)
        merger = ResultMerger(self.options.merge_timestamps, self.options.merge_profit_loss)

        results = []
        for day in days:
            day_data_reader = self.__get_data_reader(products)
            product_label = "all" if self.options.product == "all" else products[0]
            print(
                f"Backtesting {self.options.algorithm_path} "
                f"for round: {self.options.round_num} day: {day} product: {product_label}"
            )
            result = self.__run_test(trader_module, day_data_reader, self.options.round_num, day)
            results.append(result)
            SummaryPrinter.print_day_summary(result)

        if len(results) == 0:
            print("Error: no matching backtest data found")
            sys.exit(1)

        if len(results) > 1:
            SummaryPrinter.print_overall_summary(results)

        merged_result = merger.merge(results)

        if self.options.output_file is not None:
            OutputFileWriter.write_to_file(self.options.output_file, merged_result)
            print(f"\nSuccessfully saved backtest results to {self.__format_path(self.options.output_file)}")

            if self.options.show_visualizer and not merged_result.is_pnl_only():
                self.__open_visualizer()


    def __load_algorithm(self) -> Any:
        try:
            sys.path.append(str(self.options.algorithm_path.parent))
            trader_module = import_module(self.options.algorithm_path.stem)
        except ModuleNotFoundError as e:
            print(f"{self.options.algorithm_path} is not a valid algorithm file: {e}")
            sys.exit(1)

        if not hasattr(trader_module, "Trader"):
            print(f"{self.options.algorithm_path} does not expose a Trader class")
            sys.exit(1)

        return trader_module


    def __get_data_reader(self, products: list[str] | None=None) -> BackDataReader:
        return ParquetResourcesReader(self.options.back_data_dir, products)

    def __resolve_days(self, data_reader: BackDataReader) -> list[int]:
        available_days = data_reader.available_days(self.options.round_num)
        if len(available_days) == 0:
            print(f"Error: no data found for round {self.options.round_num}")
            sys.exit(1)

        if self.options.day == "all":
            return available_days

        try:
            day = int(self.options.day)
        except ValueError:
            print("Error: day must be an integer or 'all'")
            sys.exit(1)

        if day not in available_days:
            print(f"Error: no data found for round {self.options.round_num} day {day}")
            sys.exit(1)

        return [day]

    def __resolve_products(self, data_reader: ParquetResourcesReader) -> list[str]:
        available_products = data_reader.available_products(self.options.round_num)
        if len(available_products) == 0:
            print(f"Error: no products found for round {self.options.round_num}")
            sys.exit(1)

        if self.options.product == "all":
            return available_products

        if self.options.product not in available_products:
            print(f"Error: no data found for round {self.options.round_num} product {self.options.product}")
            sys.exit(1)

        return [self.options.product]

    def __run_test(self, trader_module, data_reader: BackDataReader, round: int, day: int) -> BacktestResult:
        reload(trader_module)
        self.__configure_algorithm_mode(trader_module)
        trader = trader_module.Trader()
        pnl_only = self.__is_grid_search_mode(trader_module)
        test_runner = TestRunner(
            trader,
            data_reader,
            round,
            day,
            self.options.show_progress,
            self.options.print_output,
            self.options.trade_matching_mode,
            pnl_only)
        result = test_runner.run()
        return result

    def __configure_algorithm_mode(self, trader_module) -> None:
        if not hasattr(trader_module, "TRADER_MODE"):
            print(
                f"Error: {self.options.algorithm_path} must define a module-level TRADER_MODE "
                f"(for example TRADER_MODE = SUBMISSION_MODE)"
            )
            sys.exit(1)

        mode = self.__resolve_algorithm_mode(trader_module)

        if hasattr(trader_module, "VALID_TRADER_MODES") and mode not in trader_module.VALID_TRADER_MODES:
            print(
                f"Error: TRADER_MODE {mode!r} is not valid for {self.options.algorithm_path}. "
                f"Expected one of {sorted(trader_module.VALID_TRADER_MODES)!r}"
            )
            sys.exit(1)

        if hasattr(trader_module, "set_trader_mode") and callable(trader_module.set_trader_mode):
            trader_module.set_trader_mode(mode)
        else:
            trader_module.TRADER_MODE = mode

    def __resolve_algorithm_mode(self, trader_module) -> str:
        if self.options.run_mode is None:
            return trader_module.TRADER_MODE

        mode_names = {
            RunMode.bt: getattr(trader_module, "BACKTEST_MODE", "backtest_mode"),
            RunMode.submission: getattr(trader_module, "SUBMISSION_MODE", "submission_mode"),
            RunMode.gs: getattr(trader_module, "GRID_SEARCH_MODE", "grid_search_mode"),
        }
        return mode_names[self.options.run_mode]

    def __is_grid_search_mode(self, trader_module) -> bool:
        grid_search_mode = getattr(trader_module, "GRID_SEARCH_MODE", "grid_search_mode")
        return trader_module.TRADER_MODE == grid_search_mode


    def __format_path(self, path: Path) -> str:
        cwd = Path.cwd()
        if path.is_relative_to(cwd):
            return str(path.relative_to(cwd))
        else:
            return str(path)


    def __open_visualizer(self):
        visualizer = Visualizer()
        visualizer.open(self.options.output_file)
