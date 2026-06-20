from typing import Optional
from shared.schemas import Order, OrderSide, OrderStatus, Trade
from decimal import Decimal
import uuid
import threading

class OrderBook:
    def __init__(self):
        self.bids: list[Order] = []
        self.asks: list[Order] = []
        self.orders: dict[str, Order] = {}
        self.seq_counter = 0
        self._lock = threading.Lock()
    
    def _next_seq(self) -> int:
        with self._lock:
            self.seq_counter += 1
            return self.seq_counter
    
    def add_order(self, order: Order) -> list[Trade]:
        trades: list[Trade] = []
        
        if order.seq_no == 0:
            order.seq_no = self._next_seq()
        
        if order.side == OrderSide.BUY:
            while order.quantity > 0 and self.asks:
                best_ask = self.asks[0]
                if best_ask.price > order.price:
                    break
                
                trade_quantity = min(order.quantity, best_ask.quantity)
                trade = Trade(
                    trade_id=str(uuid.uuid4()),
                    pair=order.pair,
                    buyer_id=order.account_id,
                    seller_id=best_ask.account_id,
                    buy_order_id=order.order_id,
                    sell_order_id=best_ask.order_id,
                    price=best_ask.price,
                    quantity=trade_quantity
                )
                trades.append(trade)
                
                order.quantity -= trade_quantity
                order.filled_quantity += trade_quantity
                best_ask.quantity -= trade_quantity
                best_ask.filled_quantity += trade_quantity
                
                if best_ask.quantity <= 0:
                    best_ask.status = OrderStatus.FILLED
                    self.asks.pop(0)
                    if best_ask.order_id in self.orders:
                        del self.orders[best_ask.order_id]
                else:
                    best_ask.status = OrderStatus.PARTIALLY_FILLED
        else:
            while order.quantity > 0 and self.bids:
                best_bid = self.bids[0]
                if best_bid.price < order.price:
                    break
                
                trade_quantity = min(order.quantity, best_bid.quantity)
                trade = Trade(
                    trade_id=str(uuid.uuid4()),
                    pair=order.pair,
                    buyer_id=best_bid.account_id,
                    seller_id=order.account_id,
                    buy_order_id=best_bid.order_id,
                    sell_order_id=order.order_id,
                    price=best_bid.price,
                    quantity=trade_quantity
                )
                trades.append(trade)
                
                order.quantity -= trade_quantity
                order.filled_quantity += trade_quantity
                best_bid.quantity -= trade_quantity
                best_bid.filled_quantity += trade_quantity
                
                if best_bid.quantity <= 0:
                    best_bid.status = OrderStatus.FILLED
                    self.bids.pop(0)
                    if best_bid.order_id in self.orders:
                        del self.orders[best_bid.order_id]
                else:
                    best_bid.status = OrderStatus.PARTIALLY_FILLED
        
        if order.quantity > 0:
            order.status = OrderStatus.PARTIALLY_FILLED if order.filled_quantity > 0 else OrderStatus.OPEN
            self.orders[order.order_id] = order
            if order.side == OrderSide.BUY:
                self.bids.append(order)
                self.bids.sort(key=lambda x: (-x.price, x.seq_no))
            else:
                self.asks.append(order)
                self.asks.sort(key=lambda x: (x.price, x.seq_no))
        else:
            order.status = OrderStatus.FILLED
        
        return trades
    
    def cancel_order(self, order_id: str) -> Optional[Order]:
        if order_id not in self.orders:
            return None
        
        order = self.orders[order_id]
        order.status = OrderStatus.CANCELLED
        del self.orders[order_id]
        
        if order.side == OrderSide.BUY:
            self.bids = [o for o in self.bids if o.order_id != order_id]
        else:
            self.asks = [o for o in self.asks if o.order_id != order_id]
        
        return order
    
    def get_order_status(self, order_id: str) -> Optional[dict]:
        if order_id in self.orders:
            o = self.orders[order_id]
            return {
                "order_id": o.order_id,
                "status": o.status.value,
                "filled_quantity": str(o.filled_quantity),
                "remaining_quantity": str(o.quantity)
            }
        return None
    
    def get_bids(self, depth: int = 10) -> list[dict]:
        result = []
        for order in self.bids[:depth]:
            result.append({
                "order_id": order.order_id,
                "price": str(order.price),
                "quantity": str(order.quantity),
                "account_id": order.account_id,
                "seq_no": order.seq_no
            })
        return result
    
    def get_asks(self, depth: int = 10) -> list[dict]:
        result = []
        for order in self.asks[:depth]:
            result.append({
                "order_id": order.order_id,
                "price": str(order.price),
                "quantity": str(order.quantity),
                "account_id": order.account_id,
                "seq_no": order.seq_no
            })
        return result