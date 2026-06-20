import json
import uuid
from decimal import Decimal
from nats.aio.client import Client as NATS
from shared.subjects import Subjects

class SagaOrchestrator:
    def __init__(self, nc: NATS):
        self.nc = nc
        self.known_leader = None
    
    async def _discover_leader(self):
        import httpx
        async with httpx.AsyncClient() as client:
            for wallet_url in ["http://wallet-1:8000", "http://wallet-2:8000", "http://wallet-3:8000"]:
                try:
                    resp = await client.get(f"{wallet_url}/status", timeout=2.0)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get("role") == "leader":
                            return data.get("leader_id")
                except:
                    continue
        return None
    
    async def wallet_request(self, payload: dict) -> dict:
        if not self.known_leader:
            self.known_leader = await self._discover_leader()
        
        if not self.known_leader:
            return {"error": "no_leader_available"}
        
        try:
            msg = await self.nc.request(
                f"raft.client.{self.known_leader}",
                json.dumps(payload).encode(),
                timeout=2.0
            )
            response = json.loads(msg.data)
            
            if "error" in response and response["error"] == "not_leader":
                leader_hint = response.get("leader_hint")
                if leader_hint:
                    self.known_leader = leader_hint
                    return await self.wallet_request(payload)
            
            return response
        except Exception as e:
            print(f"Request to {self.known_leader} failed: {e}")
            self.known_leader = None
            new_leader = await self._discover_leader()
            if new_leader:
                self.known_leader = new_leader
                return await self.wallet_request(payload)
            return {"error": str(e)}
    
    async def place_order(self, account_id: str, pair: str, side: str,
                          price: Decimal, quantity: Decimal) -> dict:
        order_id = str(uuid.uuid4())
        base_token, quote_token = pair.split("_")

        if side == "BUY":
            token_to_lock = quote_token
            amount_to_lock = price * quantity
        else:
            token_to_lock = base_token
            amount_to_lock = quantity

        lock_result = await self.wallet_request({
            "op": "lock",
            "ref_id": order_id,
            "account_id": account_id,
            "token": token_to_lock,
            "amount": str(amount_to_lock),
            "idempotency_key": f"lock_{order_id}"
        })

        if "error" in lock_result:
            return lock_result

        order_payload = {
            "order_id": order_id,
            "account_id": account_id,
            "pair": pair,
            "side": side,
            "price": str(price),
            "quantity": str(quantity)
        }

        subject = Subjects.order_place(pair)
        print(f"[saga] Publishing to {subject}: {order_payload}")
        await self.nc.publish(
            subject,
            json.dumps(order_payload).encode()
        )

        return {"order_id": order_id, "status": "placed"}

    async def cancel_order(self, order_id: str, pair: str) -> dict:
        release_result = await self.wallet_request({
            "op": "release",
            "ref_id": order_id,
            "idempotency_key": f"release_{order_id}"
        })

        if "error" in release_result and release_result["error"] != "unknown_ref":
            return release_result

        await self.nc.publish(
            Subjects.order_cancel(pair),
            json.dumps({"order_id": order_id}).encode()
        )

        return {"status": "cancelled"}

    async def commit_trade(self, trade_data: dict):
        trade_id = trade_data["trade_id"]
        buyer_id = trade_data["buyer_id"]
        seller_id = trade_data["seller_id"]
        buy_order_id = trade_data["buy_order_id"]
        sell_order_id = trade_data["sell_order_id"]
        pair = trade_data["pair"]
        price = Decimal(str(trade_data["price"]))
        quantity = Decimal(str(trade_data["quantity"]))
        base_token, quote_token = pair.split("_")

        print(f"[saga] Committing trade {trade_id} for pair {pair}")

        # Используем COMMIT вместо TRANSFER
        await self.wallet_request({
            "op": "commit",
            "ref_id": buy_order_id,
            "account_id": buyer_id,
            "token": quote_token,
            "amount": str(price * quantity),
            "debit_id": buyer_id,
            "credit_id": seller_id,
            "idempotency_key": f"commit_quote_{trade_id}"
        })

        await self.wallet_request({
            "op": "commit",
            "ref_id": sell_order_id,
            "account_id": seller_id,
            "token": base_token,
            "amount": str(quantity),
            "debit_id": seller_id,
            "credit_id": buyer_id,
            "idempotency_key": f"commit_base_{trade_id}"
        })