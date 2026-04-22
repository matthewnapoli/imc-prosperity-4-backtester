from __future__ import annotations

import argparse
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.csv as pv
    import pyarrow.parquet as pq
except ModuleNotFoundError as exc:
    raise SystemExit(
        "This converter requires pyarrow. Install it with:\n"
        "  python -m pip install pyarrow"
    ) from exc


ROUND_DIR_PATTERN = re.compile(r"^round\d+$")
CSV_PATTERN = re.compile(r"^(prices|trades)_round_\d+_day_-?\d+\.csv$")


@dataclass(frozen=True)
class ConversionStats:
    csv_files: int = 0
    parquet_files: int = 0

    def add(self, other: "ConversionStats") -> "ConversionStats":
        return ConversionStats(
            csv_files=self.csv_files + other.csv_files,
            parquet_files=self.parquet_files + other.parquet_files,
        )


def convert_resources(
    input_root: Path,
    output_root: Path,
    rounds: set[str] | None = None,
    clean: bool = False,
    compression: str = "snappy",
) -> ConversionStats:
    input_root = input_root.resolve()
    output_root = output_root.resolve()

    if not input_root.is_dir():
        raise FileNotFoundError(f"Input resources directory not found: {input_root}")

    if clean and output_root.exists():
        shutil.rmtree(output_root)

    output_root.mkdir(parents=True, exist_ok=True)

    stats = ConversionStats()
    for round_dir in sorted(input_root.iterdir(), key=lambda path: path.name):
        if not round_dir.is_dir() or ROUND_DIR_PATTERN.match(round_dir.name) is None:
            continue
        if rounds is not None and round_dir.name not in rounds:
            continue

        stats = stats.add(convert_round(round_dir, output_root / round_dir.name, compression))

    return stats


def convert_round(round_dir: Path, output_round_dir: Path, compression: str) -> ConversionStats:
    product_order = product_order_for_round(round_dir)
    if len(product_order) > 0:
        output_round_dir.mkdir(parents=True, exist_ok=True)
        (output_round_dir / "_products.txt").write_text("\n".join(product_order) + "\n", encoding="utf-8")

    stats = ConversionStats()
    for csv_path in sorted(round_dir.glob("*.csv"), key=lambda path: path.name):
        if CSV_PATTERN.match(csv_path.name) is None:
            continue

        parquet_count = convert_csv(csv_path, output_round_dir, compression)
        stats = stats.add(ConversionStats(csv_files=1, parquet_files=parquet_count))

    return stats


def product_order_for_round(round_dir: Path) -> list[str]:
    products = []
    seen_products = set()

    for csv_path in sorted(round_dir.glob("prices_*.csv"), key=lambda path: path.name):
        with csv_path.open("r", encoding="utf-8") as file:
            header = file.readline().rstrip("\n").split(";")
            product_index = header.index("product")
            for line in file:
                columns = line.rstrip("\n").split(";")
                product = columns[product_index]
                if product not in seen_products:
                    seen_products.add(product)
                    products.append(product)

    return products


def convert_csv(csv_path: Path, output_round_dir: Path, compression: str) -> int:
    table = pv.read_csv(csv_path, parse_options=pv.ParseOptions(delimiter=";"))
    table = table.append_column("_source_row", pa.array(range(table.num_rows), type=pa.int64()))
    product_column = product_column_for(csv_path)
    products = sorted(product for product in pc.unique(table[product_column]).to_pylist() if product)

    parquet_count = 0
    for product in products:
        product_dir_name = safe_path_part(str(product))
        output_product_dir = output_round_dir / product_dir_name
        output_product_dir.mkdir(parents=True, exist_ok=True)

        product_table = table.filter(pc.equal(table[product_column], product))
        output_file = output_product_dir / f"{csv_path.stem}_{product_dir_name}.parquet"
        pq.write_table(product_table, output_file, compression=compression)
        parquet_count += 1

    return parquet_count


def product_column_for(csv_path: Path) -> str:
    if csv_path.name.startswith("prices_"):
        return "product"
    if csv_path.name.startswith("trades_"):
        return "symbol"

    raise ValueError(f"Cannot determine product column for {csv_path}")


def safe_path_part(value: str) -> str:
    safe_value = re.sub(r'[<>:"/\\|?*\s]+', "_", value.strip())
    safe_value = safe_value.strip("._")
    if not safe_value:
        raise ValueError(f"Cannot build output path for product name {value!r}")

    return safe_value


def parse_rounds(round_values: list[int]) -> set[str] | None:
    if len(round_values) == 0:
        return None

    return {f"round{round_num}" for round_num in round_values}


def main() -> None:
    package_root = Path(__file__).resolve().parents[1]

    parser = argparse.ArgumentParser(
        description=(
            "Convert round*/prices_*.csv and trades_*.csv files into "
            "product-partitioned Parquet files under resources."
        )
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=package_root / "resources_csv",
        help="Directory containing CSV round folders. Defaults to prosperity4bt/resources_csv.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=package_root / "resources",
        help="Directory to write Parquet files into. Defaults to prosperity4bt/resources.",
    )
    parser.add_argument(
        "--round",
        dest="rounds",
        action="append",
        type=int,
        default=[],
        help="Round number to convert. Can be passed multiple times. Defaults to all rounds.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete the output directory before writing converted files.",
    )
    parser.add_argument(
        "--compression",
        default="snappy",
        help="Parquet compression codec to use. Defaults to snappy.",
    )
    args = parser.parse_args()

    stats = convert_resources(
        input_root=args.input_root,
        output_root=args.output_root,
        rounds=parse_rounds(args.rounds),
        clean=args.clean,
        compression=args.compression,
    )

    print(
        f"Converted {stats.csv_files} CSV files into {stats.parquet_files} Parquet files "
        f"under {args.output_root}"
    )


if __name__ == "__main__":
    main()
