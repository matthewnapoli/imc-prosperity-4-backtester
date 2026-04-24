import json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from prosperity4bt.models.output import BacktestResult


class OutputFileWriter:

    @staticmethod
    def write_to_file(output_file: Path, result: BacktestResult):
        output_file.parent.mkdir(parents=True, exist_ok=True)
        if result.is_pnl_only():
            table = pa.Table.from_pylist([row.to_dict() for row in result.pnl_rows])
            pq.write_table(table, output_file)
            return

        with output_file.open("w+", encoding="utf-8") as file:
            file.write(json.dumps(result.to_dict()))
