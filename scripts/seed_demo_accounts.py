import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import json
import uuid
import httpx
from nats.aio.client import Client as NATS

async def discover_leader():
    async with httpx.AsyncClient() as client:
        for wallet_url in ["http://localhost:8001", "http://localhost:8002", "http://localhost:8003"]:
            try:
                resp = await client.get(f"{wallet_url}/status", timeout=2.0)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("role") == "leader":
                        return data.get("leader_id")
            except:
                continue
    return None

async def seed():
    nc = NATS()
    await nc.connect("nats://localhost:4222")

    leader_id = await discover_leader()
    if not leader_id:
        print("No leader found")
        await nc.close()
        return

    print(f"Discovered leader: {leader_id}")

    users = ["alice", "bob", "carol", "dave", "erin"]
    tokens = {"BTC": "10", "ETH": "100", "LEARN": "10000"}

    for user in users:
        for token, amount in tokens.items():
            payload = {
                "op": "mint",
                "account_id": user,
                "token": token,
                "amount": amount,
                "ref_id": str(uuid.uuid4()),
                "idempotency_key": f"mint_{user}_{token}"
            }
            
            try:
                msg = await nc.request(
                    f"raft.client.{leader_id}",
                    json.dumps(payload).encode(),
                    timeout=2.0
                )
                response = json.loads(msg.data)
                print(f"Minted {amount} {token} for {user}: {response}")
            except Exception as e:
                print(f"Failed to mint for {user}: {e}")

    await nc.close()

if __name__ == "__main__":
    asyncio.run(seed())