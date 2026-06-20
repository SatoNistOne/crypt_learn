import json
from nats.aio.client import Client as NATS
from shared.schemas import Order
from shared.subjects import Subjects
from services.matching_engine.matcher import Matcher

class MatchingEngineHandlers:
    def __init__(self, nc: NATS, matcher: Matcher, trading_pair: str):
        self.nc = nc
        self.matcher = matcher
        self.trading_pair = trading_pair

    async def handle_order(self, msg):
        order = Order(**json.loads(msg.data))
        self.matcher.orderbook.add_order(order)
        new_trades = self.matcher.match()
        for trade in new_trades:
            await self.nc.publish(
                Subjects.trade_exec(self.trading_pair),
                trade.model_dump_json().encode()
            )

    async def handle_cancel(self, msg):
        data = json.loads(msg.data)
        self.matcher.orderbook.cancel_order(data["order_id"])