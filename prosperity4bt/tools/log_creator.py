from prosperity4bt.datamodel import TradingState
from prosperity4bt.models.output import ActivityLogRow
from prosperity4bt.models.input import BacktestData


class ActivityLogCreator:

    def __init__(self, state: TradingState, data: BacktestData, day_num: int):
        self.state = state
        self.data = data
        self.day_num = day_num

    def create_log(self) -> list[ActivityLogRow]:

        result = []

        for product in self.data.products:
            row = self.data.prices[self.state.timestamp][product]

            product_profit_loss = self.data.profit_loss[product]
            fair_value = row.fair_value if row.fair_value is not None else row.mid_price

            position = self.state.position.get(product, 0)
            if position != 0:
                product_profit_loss += position * fair_value

            bid_prices_len = len(row.bid_prices)
            bid_volumes_len = len(row.bid_volumes)
            ask_prices_len = len(row.ask_prices)
            ask_volumes_len = len(row.ask_volumes)

            columns = [
                self.day_num,
                self.state.timestamp,
                product,
                row.bid_prices[0] if bid_prices_len > 0 else "",
                row.bid_volumes[0] if bid_volumes_len > 0 else "",
                row.bid_prices[1] if bid_prices_len > 1 else "",
                row.bid_volumes[1] if bid_volumes_len > 1 else "",
                row.bid_prices[2] if bid_prices_len > 2 else "",
                row.bid_volumes[2] if bid_volumes_len > 2 else "",
                row.ask_prices[0] if ask_prices_len > 0 else "",
                row.ask_volumes[0] if ask_volumes_len > 0 else "",
                row.ask_prices[1] if ask_prices_len > 1 else "",
                row.ask_volumes[1] if ask_volumes_len > 1 else "",
                row.ask_prices[2] if ask_prices_len > 2 else "",
                row.ask_volumes[2] if ask_volumes_len > 2 else "",
                fair_value,
                product_profit_loss,
            ]

            result.append(ActivityLogRow(columns))

        return result
