from fastapi import FastAPI, HTTPException
from nats.aio.client import Client as NATS
from shared.config import Config
from shared.schemas import StatusResponse
from shared.subjects import Subjects
from services.wallet.ledger import Ledger
from services.wallet.consensus import RaftConsensus
from services.wallet.hash_chain import verify_chain

app = FastAPI()
config = Config()
nc = NATS()
ledger = Ledger(config.node_id)

peers = [p.strip() for p in config.peers.split(",") if p.strip() and p.strip() != config.node_id]
consensus = RaftConsensus(nc, ledger, config.node_id, peers)

is_ready = False

@app.on_event("startup")
async def startup():
    global is_ready
    await ledger.init_db()
    await nc.connect(config.nats_url)
    
    await nc.subscribe(
        f"raft.client.{config.node_id}",
        cb=consensus.handle_client_request
    )
    await nc.subscribe(
        Subjects.WALLET_REPLICATE,
        cb=consensus.handle_replication
    )
    await nc.subscribe(
        f"raft.resync.{config.node_id}",
        cb=consensus.handle_resync
    )
    
    await consensus.start()
    
    import asyncio
    asyncio.create_task(_startup_resync())

async def _startup_resync():
    global is_ready
    import asyncio
    for attempt in range(15):
        await asyncio.sleep(2)
        if consensus.leader_id and consensus.leader_id != config.node_id:
            await consensus.resync_from_leader()
            is_ready = True
            print(f"[{config.node_id}] Ready - synced with leader {consensus.leader_id}")
            break
        elif consensus.leader_id == config.node_id:
            is_ready = True
            print(f"[{config.node_id}] Ready - I am the leader")
            break
    else:
        is_ready = True

@app.get("/health")
async def health():
    if not is_ready:
        raise HTTPException(status_code=503, detail="not_ready")
    return {"status": "ok", "ready": is_ready}

@app.get("/status", response_model=StatusResponse)
async def status():
    raft_status = consensus.get_status()
    return StatusResponse(
        role=raft_status["role"],
        term=raft_status["term"],
        epoch=config.epoch,
        last_seq=len(ledger.journal),
        leader_id=raft_status["leader_id"]
    )

@app.get("/balances/{account_id}")
async def get_balances(account_id: str):
    balances = ledger.get_all_balances(account_id)
    return {token: {"available": str(b.available), "locked": str(b.locked)}
            for token, b in balances.items()}

@app.get("/verify-chain")
async def verify_chain_endpoint():
    valid, failed_at = verify_chain(ledger.journal)
    return {"valid": valid, "failed_at": failed_at, "length": len(ledger.journal)}