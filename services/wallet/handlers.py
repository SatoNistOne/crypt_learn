from nats.aio.client import Client as NATS
from shared.subjects import Subjects
from services.wallet.consensus import FallbackConsensus

async def setup_handlers(nc: NATS, consensus: FallbackConsensus, is_leader: bool):
    if is_leader:
        await nc.subscribe(Subjects.WALLET_CLIENT, cb=consensus.handle_client_request)
        await nc.subscribe(Subjects.WALLET_RESYNC, cb=consensus.handle_resync)
    await nc.subscribe(Subjects.WALLET_REPLICATE, cb=consensus.handle_replication)