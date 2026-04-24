import uuid

import orjson
from typing import Any
from dataclasses import dataclass
from prosperity4bt.datamodel import Trade


@dataclass
class SandboxLogRow:
    timestamp: int
    sandbox_log: str
    lambda_log: str

    def __init__(self, timestamp: int, sandbox_log: str, lambda_log: str):
        self.timestamp = timestamp
        self.sandbox_log = sandbox_log
        self.lambda_log = lambda_log

    def with_offset(self, timestamp_offset: int) -> "SandboxLogRow":
        return SandboxLogRow(
            self.timestamp + timestamp_offset,
            self.sandbox_log,
            self.lambda_log.replace(f"[[{self.timestamp},", f"[[{self.timestamp + timestamp_offset},"),
        )

    def __str__(self) -> str:
        return orjson.dumps(
            {
                "sandboxLog": self.sandbox_log,
                "lambdaLog": self.lambda_log,
                "timestamp": self.timestamp,
            },
            option=orjson.OPT_APPEND_NEWLINE | orjson.OPT_INDENT_2,
        ).decode("utf-8")

    def to_dict(self):
        return {
            "sandboxLog": self.sandbox_log,
            "lambdaLog": self.lambda_log,
            "timestamp": self.timestamp
        }


@dataclass
class ActivityLogRow:
    columns: list[Any]

    @property
    def timestamp(self) -> int:
        return self.columns[1]

    @property
    def symbol(self) -> str:
        return self.columns[2]

    @property
    def profit_loss(self) -> float:
        return self.columns[-1]

    def with_offset(self, timestamp_offset: int, profit_loss_offset: float) -> "ActivityLogRow":
        new_columns = self.columns[:]
        new_columns[1] += timestamp_offset
        new_columns[-1] += profit_loss_offset

        return ActivityLogRow(new_columns)

    def __str__(self) -> str:
        return ";".join(map(str, self.columns))

    @staticmethod
    def get_header_str(value_column: str="mid_price") -> str:
        return f'day;timestamp;product;bid_price_1;bid_volume_1;bid_price_2;bid_volume_2;bid_price_3;bid_volume_3;ask_price_1;ask_volume_1;ask_price_2;ask_volume_2;ask_price_3;ask_volume_3;{value_column};profit_and_loss'


@dataclass
class TradeRow:
    trade: Trade
    trade_type: str = "market"

    @property
    def timestamp(self) -> int:
        return self.trade.timestamp

    def with_offset(self, timestamp_offset: int) -> "TradeRow":
        return TradeRow(
            Trade(
                self.trade.symbol,
                self.trade.price,
                self.trade.quantity,
                self.trade.buyer,
                self.trade.seller,
                self.trade.timestamp + timestamp_offset,
            ),
            self.trade_type,
        )

    def to_dict(self):
        return {
            "timestamp": self.trade.timestamp,
            "buyer": self.trade.buyer,
            "seller": self.trade.seller,
            "symbol": self.trade.symbol,
            "currency": "XIRECS",
            "price": float(self.trade.price),
            "quantity": self.trade.quantity,
            "tradeType": self.trade_type,
        }

    def __str__(self) -> str:
        return (
            "  "
            + f"""
  {{
    "timestamp": {self.trade.timestamp},
    "buyer": "{self.trade.buyer}",
    "seller": "{self.trade.seller}",
    "symbol": "{self.trade.symbol}",
    "currency": "XIRECS",
    "price": {self.trade.price},
    "quantity": {self.trade.quantity},
    "tradeType": "{self.trade_type}",
  }}
        """.strip()
        )


@dataclass
class PnlRow:
    round_num: int
    day_num: int
    timestamp: int
    pnl: float

    def with_offset(self, timestamp_offset: int, pnl_offset: float) -> "PnlRow":
        return PnlRow(
            self.round_num,
            self.day_num,
            self.timestamp + timestamp_offset,
            self.pnl + pnl_offset,
        )

    def to_dict(self) -> dict[str, float | int]:
        return {
            "round": self.round_num,
            "day": self.day_num,
            "timestamp": self.timestamp,
            "pnl": self.pnl,
        }


@dataclass
class BacktestResult:
    round_num: int
    day_num: int
    last_day_num: int
    sandbox_logs: list[SandboxLogRow]
    activity_logs: list[ActivityLogRow]
    trades: list[TradeRow]
    pnl_rows: list[PnlRow]
    activity_value_column: str

    def __init__(self, round_num: int, day_num: int, sandbox_logs: list[SandboxLogRow]=None, activity_logs: list[ActivityLogRow]=None, trades: list[TradeRow]=None, activity_value_column: str="mid_price", pnl_rows: list[PnlRow]=None):
        self.round_num = round_num
        self.day_num = day_num
        self.last_day_num = day_num
        self.sandbox_logs = sandbox_logs if sandbox_logs is not None else []
        self.activity_logs = activity_logs if activity_logs is not None else []
        self.trades = trades if trades is not None else []
        self.pnl_rows = pnl_rows if pnl_rows is not None else []
        self.activity_value_column = activity_value_column

    # return a list of activities that happened at the end of the day, i.e. last timestamp
    def final_activities(self) -> list[ActivityLogRow]:
        if len(self.activity_logs) == 0:
            return []
        last_time_stamp = self.activity_logs[-1].timestamp
        return [activity for activity in self.activity_logs if activity.timestamp == last_time_stamp]

    def is_pnl_only(self) -> bool:
        return (
            len(self.pnl_rows) > 0
            and len(self.sandbox_logs) == 0
            and len(self.activity_logs) == 0
            and len(self.trades) == 0
        )

    def to_dict(self) -> dict:
        if self.is_pnl_only():
            return {"pnl": [row.to_dict() for row in self.pnl_rows]}

        return {
            "submissionId": str(uuid.uuid4()),
            "activitiesLog": ActivityLogRow.get_header_str(self.activity_value_column) + '\n' +'\n'.join([str(al) for al in self.activity_logs]),
            "logs": [sl.to_dict() for sl in self.sandbox_logs],
            "tradeHistory": [t.to_dict() for t in self.trades]
        }
