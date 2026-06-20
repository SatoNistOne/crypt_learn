import json
from nats.aio.client import Client as NATS
from services.api_gateway.saga import SagaOrchestrator

class TradeHandler:
    def __init__(self, nc: NATS, saga: SagaOrchestrator):
        self.nc = nc
        self.saga = saga

    async def handle_trade(self, msg):
        trade_data = json.loads(msg.data)
        await self.saga.commit_trade(trade_data)