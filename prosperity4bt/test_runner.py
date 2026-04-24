from contextlib import closing, redirect_stdout
from io import StringIO
from IPython.utils.io import Tee
from tqdm import tqdm
from prosperity4bt.constants import LIMITS
from prosperity4bt.models.test_options import TradeMatchingMode
from prosperity4bt.tools.data_reader import BackDataReader
from prosperity4bt.datamodel import TradingState, Observation, Symbol, Order, OrderDepth, Listing, ConversionObservation
from prosperity4bt.tools.log_creator import ActivityLogCreator
from prosperity4bt.models.input import BacktestData
from prosperity4bt.models.output import BacktestResult
from prosperity4bt.models.output import PnlRow
from prosperity4bt.models.output import SandboxLogRow
from prosperity4bt.tools.order_match_maker import OrderMatchMaker


class TestRunner:

    def __init__(self, trader, data_reader: BackDataReader, round: int, day: int, show_progress_bar: bool=False, print_output: bool=False, trade_matching_mode=TradeMatchingMode.all, pnl_only: bool=False):
        self.trader = trader
        self.data_reader = data_reader
        self.round = round
        self.day = day
        self.show_progress_bar = show_progress_bar
        self.print_output = print_output
        self.trade_matching_mode = trade_matching_mode
        self.pnl_only = pnl_only


    def run(self):
        data = self.data_reader.read_from_file(self.round, self.day)
        self.__print_fair_value_sources(data)
        state = TradingState(
            traderData="",
            timestamp=0,
            listings={},
            order_depths={},
            own_trades={},
            market_trades={},
            position={},
            observations=Observation({}, {}),
        )
        result = BacktestResult(data.round_num, data.day_num, activity_value_column=self.__activity_value_column(data))

        timestamps = sorted(data.prices.keys())
        timestamps_iterator = tqdm(timestamps, ascii=True) if self.show_progress_bar else timestamps
        for timestamp in timestamps_iterator:
            state = self.__initialize_trade_state(state, data, timestamp)
            orders = self.__run_trader(state, result, timestamp)

            sandbox_row = result.sandbox_logs[-1] if len(result.sandbox_logs) > 0 else None
            self.__enforce_limits(state, data, orders, sandbox_row)
            self.__match_orders(state, data, orders, result)

            if self.pnl_only:
                self.__create_pnl_row(state, data, result)
            else:
                self.__create_activity_logs(state, data, result)

        return result

    def __print_fair_value_sources(self, data: BacktestData) -> None:
        fair_value_sources = data.fair_value_sources or {}
        for product in data.products:
            source = fair_value_sources.get(product, "mid_price fallback")
            print(f"{product}: using {source} for fair_value and mark-to-market PnL")

    def __activity_value_column(self, data: BacktestData) -> str:
        fair_value_sources = data.fair_value_sources or {}
        for product in data.products:
            if fair_value_sources.get(product) == "precomputed fair_value":
                return "fair_value"

        return "mid_price"

    def __run_trader(self, state: TradingState, result: BacktestResult, timestamp: int) -> dict[Symbol, list[Order]]:
        if self.pnl_only and not self.print_output:
            orders, conversions, trader_data = self.trader.run(state)
            state.traderData = trader_data
            return orders

        stdout = StringIO()
        # Tee calls stdout.close(), making stdout.getvalue() impossible
        # This override makes getvalue() possible after close()
        stdout.close = lambda: None  # type: ignore[method-assign]

        if self.print_output:
            with closing(Tee(stdout)):
                orders, conversions, trader_data = self.trader.run(state)
        else:
            with redirect_stdout(stdout):
                orders, conversions, trader_data = self.trader.run(state)

        state.traderData = trader_data

        sandbox_row = SandboxLogRow(
            timestamp=timestamp,
            sandbox_log="",
            lambda_log=stdout.getvalue().rstrip(),
        )
        result.sandbox_logs.append(sandbox_row)

        return orders


    def __initialize_trade_state(self, state: TradingState, data: BacktestData, timestamp: int) -> TradingState:
        state.timestamp = timestamp
        state.listings = {product: Listing(product, product, 1) for product in data.products}
        for product in sorted(data.products):
            order_depth = OrderDepth()
            row = data.prices[state.timestamp][product]

            for price, volume in zip(row.bid_prices, row.bid_volumes):
                order_depth.buy_orders[price] = volume

            for price, volume in zip(row.ask_prices, row.ask_volumes):
                order_depth.sell_orders[price] = -volume

            state.order_depths[product] = order_depth

        observation_row = data.observations.get(state.timestamp)
        if observation_row is None:
            state.observations = Observation({}, {})
        else:
            conversion_observation = ConversionObservation(
                bidPrice=observation_row.bidPrice,
                askPrice=observation_row.askPrice,
                transportFees=observation_row.transportFees,
                exportTariff=observation_row.exportTariff,
                importTariff=observation_row.importTariff,
                sugarPrice=observation_row.sugarPrice,
                sunlightIndex=observation_row.sunlightIndex,
            )
            state.observations = Observation(
                plainValueObservations={}, conversionObservations={"MAGNIFICENT_MACARONS": conversion_observation}
            )

        return state

    # def __validate_orders(self, orders: dict[Symbol, list[Order]]) -> None:
    #     for key, value in orders.items():
    #         if not isinstance(key, str):
    #             raise ValueError(f"Orders key '{key}' is of type {type(key)}, expected a str")
    #         for order in value:
    #             if not isinstance(order.symbol, str):
    #                 raise ValueError(f"Order symbol of '{order}' is of type {type(order.symbol)}, expected a str")
    #             if not isinstance(order.price, int):
    #                 raise ValueError(f"Order price of '{order}' is of type {type(order.price)}, expected an int")
    #             if not isinstance(order.quantity, int):
    #                 raise ValueError(f"Order quantity of '{order}' is of type {type(order.quantity)}, expected an int")


    def __create_activity_logs(self, state: TradingState, data: BacktestData, result: BacktestResult,) -> None:
        log_creator = ActivityLogCreator(state, data, result.day_num)
        log = log_creator.create_log()
        result.activity_logs.extend(log)

    def __create_pnl_row(self, state: TradingState, data: BacktestData, result: BacktestResult) -> None:
        total_pnl = 0.0
        for product in data.products:
            row = data.prices[state.timestamp][product]
            fair_value = row.fair_value if row.fair_value is not None else row.mid_price
            product_profit_loss = data.profit_loss[product]
            position = state.position.get(product, 0)
            if position != 0:
                product_profit_loss += position * fair_value
            total_pnl += product_profit_loss

        result.pnl_rows.append(PnlRow(data.round_num, result.day_num, state.timestamp, total_pnl))


    def __enforce_limits(self, state: TradingState, data: BacktestData, orders: dict[Symbol, list[Order]], sandbox_row: SandboxLogRow | None) -> None:
        sandbox_log_lines = []
        for product in data.products:
            product_orders = orders.get(product, [])
            product_position = state.position.get(product, 0)

            total_long = sum(order.quantity for order in product_orders if order.quantity > 0)
            total_short = sum(abs(order.quantity) for order in product_orders if order.quantity < 0)
            remaining_buy_capacity = LIMITS[product] - product_position
            remaining_sell_capacity = LIMITS[product] + product_position

            if total_long > remaining_buy_capacity or total_short > remaining_sell_capacity:
                sandbox_log_lines.append(f"Orders for product {product} exceeded limit of {LIMITS[product]} set")
                orders.pop(product)

        if len(sandbox_log_lines) > 0 and sandbox_row is not None:
            sandbox_row.sandbox_log += "\n" + "\n".join(sandbox_log_lines)


    def __match_orders(self, state: TradingState, data: BacktestData, orders: dict[Symbol, list[Order]], result: BacktestResult) -> None:
        match_maker = OrderMatchMaker(state, data, orders, self.trade_matching_mode)
        matched_trades = match_maker.match()
        if not self.pnl_only:
            result.trades.extend(matched_trades)
