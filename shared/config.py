import os
from dataclasses import dataclass

@dataclass
class Config:
    node_id: str = os.getenv("NODE_ID", "wallet-1")
    peers: str = os.getenv("PEERS", "wallet-1,wallet-2,wallet-3")
    is_leader: bool = os.getenv("IS_LEADER", "false").lower() == "true"
    epoch: int = int(os.getenv("EPOCH", "1"))
    nats_url: str = os.getenv("NATS_URL", "nats://nats:4222")
    trading_pair: str = os.getenv("TRADING_PAIR", "BTC_LEARN")