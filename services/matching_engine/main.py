import os
import json
import asyncio
from fastapi import FastAPI
from nats.aio.client import Client as NATS
from shared.config import Config
from shared.subjects import Subjects
from shared.schemas import Order, Trade
from services.matching_engine.orderbook import OrderBook
import uuid

app = FastAPI()
config = Config()
nc = NATS()
orderbook = OrderBook()
trades_history: list[dict] = []

@app.on_event("startup")
async def startup():
    await nc.connect(config.nats_url)
    pair = config.trading_pair
    await nc.subscribe(Subjects.order_place(pair), cb=handle_order)
    await nc.subscribe(Subjects.order_cancel(pair), cb=handle_cancel)
    print(f"[matching-{pair}] Started for pair {pair}")

async def handle_order(msg):
    try:
        data = json.loads(msg.data)
        order = Order(**data)
        trades = orderbook.add_order(order)
        
        print(f"[matching-{config.trading_pair}] Received order: {order.order_id}, side={order.side}, price={order.price}, qty={order.quantity}")
        
        for trade in trades:
            trade_data = {
                "trade_id": str(uuid.uuid4()),
                "pair": trade.pair,
                "buyer_id": trade.buyer_id,
                "seller_id": trade.seller_id,
                "buy_order_id": trade.buy_order_id,
                "sell_order_id": trade.sell_order_id,
                "price": str(trade.price),
                "quantity": str(trade.quantity)
            }
            trades_history.append(trade_data)
            print(f"[matching-{config.trading_pair}] Trade executed: {trade_data}")
            await nc.publish(
                Subjects.trade_exec(order.pair),
                json.dumps(trade_data).encode()
            )
    except json.JSONDecodeError as e:
        print(f"[matching-{config.trading_pair}] Invalid JSON: {e}, data: {msg.data}")
    except Exception as e:
        print(f"[matching-{config.trading_pair}] Error processing order: {e}")

async def handle_cancel(msg):
    try:
        data = json.loads(msg.data)
        orderbook.cancel_order(data["order_id"])
    except Exception as e:
        print(f"[matching-{config.trading_pair}] Error canceling order: {e}")

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/status")
async def status():
    return {
        "pair": config.trading_pair,
        "orders_count": len(orderbook.bids) + len(orderbook.asks),
        "trades_count": len(trades_history)
    }

@app.get("/orderbook")
async def get_orderbook(depth: int = 10):
    return {
        "pair": config.trading_pair,
        "bids": orderbook.get_bids(depth),
        "asks": orderbook.get_asks(depth)
    }

@app.get("/trades")
async def get_trades(limit: int = 50):
    return {"pair": config.trading_pair, "trades": trades_history[-limit:]}