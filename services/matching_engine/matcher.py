import uuid
from decimal import Decimal
from shared.schemas import Trade
from services.matching_engine.orderbook import OrderBook

class Matcher:
    def __init__(self, orderbook: OrderBook, trading_pair: str):
        self.orderbook = orderbook
        self.trading_pair = trading_pair
        self.trades: list[Trade] = []

    def match(self) -> list[Trade]:
        new_trades = []

        while True:
            best_bid = self.orderbook.get_best_bid()
            best_ask = self.orderbook.get_best_ask()

            if not best_bid or not best_ask:
                break

            bid_price, bid_id = best_bid
            ask_price, ask_id = best_ask

            if bid_price < ask_price:
                break

            bid_order = self.orderbook.orders[bid_id]
            ask_order = self.orderbook.orders[ask_id]

            fill_qty = min(bid_order.quantity, ask_order.quantity)

            trade = Trade(
                trade_id=str(uuid.uuid4()),
                pair=self.trading_pair,
                buyer_id=bid_order.account_id,
                seller_id=ask_order.account_id,
                buy_order_id=bid_id,
                sell_order_id=ask_id,
                price=ask_price,
                quantity=fill_qty
            )

            new_trades.append(trade)
            self.trades.append(trade)

            bid_order.quantity -= fill_qty
            ask_order.quantity -= fill_qty

        return new_trades