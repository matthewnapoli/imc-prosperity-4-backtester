from abc import abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from importlib import resources
from pathlib import Path
import re
from typing import ContextManager, Optional
from prosperity4bt.datamodel import Trade, Symbol
from prosperity4bt.models.input import PriceRow, ObservationRow, BacktestData


class BackDataReader:
    def __init__(self, products: Optional[list[str]]=None):
        self.products = set(products) if products else None

    def read_from_file(self, round_num: int, day_num: int) -> BacktestData:
        prices = self.__get_prices(round_num, day_num)
        trades = self.__get_trades(round_num, day_num)
        observations = self.__get_observations(round_num, day_num)

        products = []
        prices_by_timestamp: dict[int, dict[Symbol, PriceRow]] = defaultdict(dict)
        for row in prices:
            prices_by_timestamp[row.timestamp][row.product] = row
            if row.product not in products:
                products.append(row.product)

        trades_by_timestamp: dict[int, dict[Symbol, list[Trade]]] = defaultdict(lambda: defaultdict(list))
        for trade in trades:
            trades_by_timestamp[trade.timestamp][trade.symbol].append(trade)

        profit_loss = {product: 0.0 for product in products}

        observations_by_timestamp = {row.timestamp: row for row in observations}

        backtest_data = BacktestData(
            round_num=round_num,
            day_num=day_num,
            prices=prices_by_timestamp,
            trades=trades_by_timestamp,
            observations=observations_by_timestamp,
            products=products,
            profit_loss=profit_loss,
        )
        return backtest_data

    def _include_product(self, product: Symbol) -> bool:
        return self.products is None or product in self.products

    def __get_prices(self, round_num: int, day_num: int):
        prices = []
        with self._read_file_content([f"round{round_num}", f"prices_round_{round_num}_day_{day_num}.csv"]) as file:
            if file is None:
                raise ValueError(f"Prices data is not available for round {round_num} day {day_num}")

            for line in file.read_text(encoding="utf-8").splitlines()[1:]:
                price_row = PriceRow.parse_from_str(line)
                if self._include_product(price_row.product):
                    prices.append(price_row)
        return prices

    def __get_trades(self, round_num: int, day_num: int):
        trades = []
        with self._read_file_content([f"round{round_num}", f"trades_round_{round_num}_day_{day_num}.csv"]) as file:
            if file is not None:
                for line in file.read_text(encoding="utf-8").splitlines()[1:]:
                    columns = line.split(";")
                    trades.append(
                        Trade(
                            symbol=columns[3],
                            price=int(float(columns[5])),
                            quantity=int(columns[6]),
                            buyer=columns[1],
                            seller=columns[2],
                            timestamp=int(columns[0]),
                        )
                    )
                    if not self._include_product(trades[-1].symbol):
                        trades.pop()
        return trades

    def __get_observations(self, round_num: int, day_num: int):
        observations = []
        with self._read_file_content([f"round{round_num}", f"observations_round_{round_num}_day_{day_num}.csv"]) as file:
            if file is not None:
                for line in file.read_text(encoding="utf-8").splitlines()[1:]:
                    observations.append(ObservationRow.parse_from_str(line))
        return observations

    @abstractmethod
    def available_days(self, round: int) -> list[int]:
        if round == 0:
            return [-2, -1]
        if round == 1:
            return [-2, -1, 0]
        if round == 2:
            return [-1, 0, 1]
        if round == 3:
            return [0, 1, 2]
        if round == 4:
            return [1, 2, 3]
        if round == 5:
            return [2, 3, 4]
        return []

    @abstractmethod
    def _read_file_content(self, path_parts: list[str]) -> ContextManager[Optional[Path]]:
        """Given a path to a file, yields a single Path object to the file or None if the file does not exist."""
        raise NotImplementedError()


class PackageResourcesReader(BackDataReader):
    def available_days(self, round: int) -> list[int]:
        discovered_days = self.__available_days_from_resources(round)
        if len(discovered_days) > 0:
            return discovered_days

        return super().available_days(round)

    def _read_file_content(self, path_parts: list[str]) -> ContextManager[Optional[Path]]:
        try:
            file_path = f"prosperity4bt.resources.{'.'.join(path_parts[:-1])}"
            container = resources.files(file_path)
            file = container / path_parts[-1]
            if not file.is_file():
                return wrap_in_context_manager(None)

            return resources.as_file(file)
        except Exception:
            return wrap_in_context_manager(None)

    def __available_days_from_resources(self, round: int) -> list[int]:
        try:
            container = resources.files(f"prosperity4bt.resources.round{round}")
        except Exception:
            return []

        pattern = re.compile(rf"^prices_round_{round}_day_(-?\d+)\.csv$")
        days = []
        for file in container.iterdir():
            match = pattern.match(file.name)
            if match is not None:
                days.append(int(match.group(1)))

        return sorted(days)


class ParquetResourcesReader(BackDataReader):
    def __init__(self, data_dir: Optional[Path]=None, products: Optional[list[str]]=None):
        super().__init__(products)
        self.data_dir = data_dir

    def read_from_file(self, round_num: int, day_num: int) -> BacktestData:
        prices = self.__get_prices(round_num, day_num)
        if len(prices) == 0:
            raise ValueError(f"Prices data is not available for round {round_num} day {day_num}")

        trades = self.__get_trades(round_num, day_num)

        products = []
        prices_by_timestamp: dict[int, dict[Symbol, PriceRow]] = defaultdict(dict)
        for row in prices:
            prices_by_timestamp[row.timestamp][row.product] = row
            if row.product not in products:
                products.append(row.product)

        trades_by_timestamp: dict[int, dict[Symbol, list[Trade]]] = defaultdict(lambda: defaultdict(list))
        for trade in trades:
            trades_by_timestamp[trade.timestamp][trade.symbol].append(trade)

        return BacktestData(
            round_num=round_num,
            day_num=day_num,
            prices=prices_by_timestamp,
            trades=trades_by_timestamp,
            observations={},
            products=products,
            profit_loss={product: 0.0 for product in products},
        )

    def available_days(self, round: int) -> list[int]:
        round_root = self.__round_root(round)
        if round_root is None:
            return []

        pattern = re.compile(rf"^prices_round_{round}_day_(-?\d+)_.+\.parquet$")
        days = set()
        for product_dir in self.__product_dirs(round_root):
            for file in product_dir.iterdir():
                match = pattern.match(file.name)
                if match is not None:
                    days.add(int(match.group(1)))

        return sorted(days)

    def available_products(self, round: int) -> list[str]:
        round_root = self.__round_root(round)
        if round_root is None:
            return []

        return [product_dir.name for product_dir in self.__product_dirs(round_root)]

    def _read_file_content(self, path_parts: list[str]) -> ContextManager[Optional[Path]]:
        return wrap_in_context_manager(None)

    def __get_prices(self, round_num: int, day_num: int) -> list[PriceRow]:
        rows = []
        for file in self.__matching_files(round_num, day_num, "prices"):
            table = self.__read_parquet(file)
            for row in table.to_pylist():
                rows.append(
                    PriceRow(
                        day=int(row["day"]),
                        timestamp=int(row["timestamp"]),
                        product=row["product"],
                        bid_prices=self.__compact_ints(row, ["bid_price_1", "bid_price_2", "bid_price_3"]),
                        bid_volumes=self.__compact_ints(row, ["bid_volume_1", "bid_volume_2", "bid_volume_3"]),
                        ask_prices=self.__compact_ints(row, ["ask_price_1", "ask_price_2", "ask_price_3"]),
                        ask_volumes=self.__compact_ints(row, ["ask_volume_1", "ask_volume_2", "ask_volume_3"]),
                        mid_price=float(row["mid_price"]),
                        profit_loss=float(row["profit_and_loss"]),
                    )
                )

        return rows

    def __get_trades(self, round_num: int, day_num: int) -> list[Trade]:
        rows = []
        for file in self.__matching_files(round_num, day_num, "trades"):
            table = self.__read_parquet(file)
            rows.extend(table.to_pylist())

        rows.sort(key=lambda row: row.get("_source_row", 0))
        return [
            Trade(
                symbol=row["symbol"],
                price=int(float(row["price"])),
                quantity=int(row["quantity"]),
                buyer=row["buyer"] or "",
                seller=row["seller"] or "",
                timestamp=int(row["timestamp"]),
            )
            for row in rows
        ]

    def __matching_files(self, round_num: int, day_num: int, kind: str) -> list[Path]:
        round_root = self.__round_root(round_num)
        if round_root is None:
            return []

        files = []
        pattern = f"{kind}_round_{round_num}_day_{day_num}_*.parquet"
        for product_dir in self.__product_dirs(round_root):
            files.extend(sorted(product_dir.glob(pattern), key=lambda path: path.name))

        return files

    def __product_dirs(self, round_root: Path) -> list[Path]:
        product_names = self.__product_order(round_root)
        if len(product_names) > 0:
            return [
                round_root / product
                for product in product_names
                if (round_root / product).is_dir() and self._include_product(product)
            ]

        return [
            product_dir
            for product_dir in sorted(round_root.iterdir(), key=lambda path: path.name)
            if product_dir.is_dir() and self._include_product(product_dir.name)
        ]

    @staticmethod
    def __product_order(round_root: Path) -> list[str]:
        manifest = round_root / "_products.txt"
        if not manifest.is_file():
            return []

        return [
            product.strip()
            for product in manifest.read_text(encoding="utf-8").splitlines()
            if product.strip()
        ]

    def __round_root(self, round_num: int) -> Optional[Path]:
        data_root = self.data_dir or Path(__file__).resolve().parents[1] / "resources"
        path = data_root / f"round{round_num}"
        return path if path.is_dir() else None

    def __read_parquet(self, file: Path):
        try:
            import pyarrow.parquet as pq
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Parquet resources require pyarrow. Install it with `python -m pip install pyarrow`."
            ) from exc

        return pq.read_table(file)

    @staticmethod
    def __compact_ints(row: dict, columns: list[str]) -> list[int]:
        return [int(row[column]) for column in columns if row[column] is not None]


@contextmanager
def wrap_in_context_manager(value):
    yield value
