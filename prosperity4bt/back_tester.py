import sys
from importlib import import_module, reload
from pathlib import Path
from typing import Any
from prosperity4bt.tools.data_reader import BackDataReader, ParquetResourcesReader
from prosperity4bt.models.output import BacktestResult
from prosperity4bt.tools.output_file_writer import OutputFileWriter
from prosperity4bt.tools.result_merger import ResultMerger
from prosperity4bt.tools.summary_printer import SummaryPrinter
from prosperity4bt.models.test_options import TestOptions
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
        self.__validate_day_product_selection(days, products)
        merger = ResultMerger(self.options.merge_timestamps, self.options.merge_profit_loss)

        results = []
        for day in days:
            for product in products:
                product_data_reader = self.__get_data_reader([product])
                print(
                    f"Backtesting {self.options.algorithm_path} "
                    f"for round: {self.options.round_num} day: {day} product: {product}"
                )
                result = self.__run_test(trader_module, product_data_reader, self.options.round_num, day)
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

            if self.options.show_visualizer:
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

    def __validate_day_product_selection(self, days: list[int], products: list[str]) -> None:
        if len(products) > 1:
            print(
                "Error: cannot merge multiple product runs for the same day into one log. "
                "Run one product at a time, e.g. pass PEPPER or OSMIUM."
            )
            sys.exit(1)


    def __run_test(self, trader_module, data_reader: BackDataReader, round: int, day: int) -> BacktestResult:
        reload(trader_module)
        test_runner = TestRunner(
            trader_module.Trader(),
            data_reader,
            round,
            day,
            self.options.show_progress,
            self.options.print_output,
            self.options.trade_matching_mode)
        result = test_runner.run()
        return result


    def __format_path(self, path: Path) -> str:
        cwd = Path.cwd()
        if path.is_relative_to(cwd):
            return str(path.relative_to(cwd))
        else:
            return str(path)


    def __open_visualizer(self):
        visualizer = Visualizer()
        visualizer.open(self.options.output_file)
