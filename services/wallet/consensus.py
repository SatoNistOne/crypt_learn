import json
import asyncio
import random
from decimal import Decimal
from enum import Enum
from typing import Optional
from nats.aio.client import Client as NATS
from shared.subjects import Subjects
from shared.schemas import LedgerEntry, LedgerOpType
from services.wallet.ledger import Ledger
from services.wallet.hash_chain import compute_hash, create_entry_payload

class NodeRole(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class RaftConsensus:
    def __init__(self, nc: NATS, ledger: Ledger, node_id: str, peers: list[str]):
        self.nc = nc
        self.ledger = ledger
        self.node_id = node_id
        self.peers = peers
        
        self.role = NodeRole.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None
        
        self.commit_index = 0
        self.last_applied = 0
        
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        
        self.idempotency_cache: dict[str, dict] = {}
        
        self.election_timeout = random.uniform(0.15, 0.3)
        self.heartbeat_interval = 0.05
        
        self.election_timer: Optional[asyncio.Task] = None
        self.heartbeat_timer: Optional[asyncio.Task] = None
        
        self.running = False
    
    async def start(self):
        self.running = True
        for peer in self.peers:
            self.next_index[peer] = len(self.ledger.journal) + 1
            self.match_index[peer] = 0
        
        await self._subscribe_to_raft()
        await self._reset_election_timer()
        asyncio.create_task(self._periodic_resync())
    
    async def stop(self):
        self.running = False
        if self.election_timer:
            self.election_timer.cancel()
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
    
    async def _subscribe_to_raft(self):
        await self.nc.subscribe(
            f"{Subjects.RAFT_REQUEST_VOTE}.{self.node_id}",
            cb=self._handle_request_vote
        )
        await self.nc.subscribe(
            f"raft.appendentries.{self.node_id}",
            cb=self._handle_append_entries
        )
    
    async def _reset_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()
        
        if self.role != NodeRole.LEADER:
            self.election_timeout = random.uniform(0.15, 0.3)
            self.election_timer = asyncio.create_task(self._election_timeout())
    
    async def _election_timeout(self):
        await asyncio.sleep(self.election_timeout)
        if self.running and self.role != NodeRole.LEADER:
            await self._start_election()
    
    async def _start_election(self):
        self.current_term += 1
        self.role = NodeRole.CANDIDATE
        self.voted_for = self.node_id
        
        print(f"[{self.node_id}] Starting election for term {self.current_term}")
        
        votes_needed = (len(self.peers) + 1) // 2 + 1
        votes_received = 1
        
        last_log_index = len(self.ledger.journal)
        last_log_term = self.ledger.journal[-1].epoch if self.ledger.journal else 0
        
        tasks = []
        for peer in self.peers:
            task = self._send_request_vote(peer, last_log_index, last_log_term)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, dict) and result.get("vote_granted"):
                votes_received += 1
        
        if votes_received >= votes_needed:
            self.role = NodeRole.LEADER
            self.leader_id = self.node_id
            print(f"[{self.node_id}] Became leader for term {self.current_term}")
            
            for peer in self.peers:
                self.next_index[peer] = len(self.ledger.journal) + 1
                self.match_index[peer] = 0
            
            await self._start_heartbeat()
        else:
            print(f"[{self.node_id}] Election failed, received {votes_received}/{votes_needed} votes")
            self.role = NodeRole.FOLLOWER
            self.voted_for = None
            await self._reset_election_timer()
    
    async def _send_request_vote(self, peer: str, last_log_index: int, last_log_term: int) -> dict:
        try:
            request = {
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term
            }
            msg = await self.nc.request(
                f"{Subjects.RAFT_REQUEST_VOTE}.{peer}",
                json.dumps(request).encode(),
                timeout=0.1
            )
            return json.loads(msg.data)
        except Exception as e:
            return {"vote_granted": False}
    
    async def _handle_request_vote(self, msg):
        request = json.loads(msg.data)
        
        response = {"term": self.current_term, "vote_granted": False}
        
        if request["term"] > self.current_term:
            self.current_term = request["term"]
            self.role = NodeRole.FOLLOWER
            self.voted_for = None
            if self.heartbeat_timer:
                self.heartbeat_timer.cancel()
        
        if request["term"] >= self.current_term:
            if self.voted_for is None or self.voted_for == request["candidate_id"]:
                my_last_log_term = self.ledger.journal[-1].epoch if self.ledger.journal else 0
                my_last_log_index = len(self.ledger.journal)
                
                if (request["last_log_term"] > my_last_log_term or
                    (request["last_log_term"] == my_last_log_term and 
                     request["last_log_index"] >= my_last_log_index)):
                    
                    self.voted_for = request["candidate_id"]
                    response["vote_granted"] = True
                    await self._reset_election_timer()
        
        await msg.respond(json.dumps(response).encode())
    
    async def _start_heartbeat(self):
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = asyncio.create_task(self._send_heartbeats())
    
    async def _send_heartbeats(self):
        while self.running and self.role == NodeRole.LEADER:
            tasks = []
            for peer in self.peers:
                task = self._send_append_entries(peer, empty=True)
                tasks.append(task)
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(self.heartbeat_interval)
    
    def _recompute_entry_hashes(self, entry: LedgerEntry) -> LedgerEntry:
        prev_hash = self.ledger.journal[-1].this_hash if self.ledger.journal else "0" * 64
        entry.prev_hash = prev_hash
        payload = create_entry_payload(entry)
        entry.this_hash = compute_hash(prev_hash, payload)
        return entry
    
    async def _send_append_entries(self, peer: str, empty: bool = False):
        try:
            next_idx = self.next_index.get(peer, 1)
            
            if empty:
                entries_to_send = []
            else:
                entries_to_send = []
                for e in self.ledger.journal[next_idx - 1:]:
                    entry_dict = e.model_dump()
                    entry_dict['amount'] = str(entry_dict['amount'])
                    entries_to_send.append(entry_dict)
            
            prev_log_index = next_idx - 1
            prev_log_term = (
                self.ledger.journal[prev_log_index - 1].epoch 
                if prev_log_index > 0 and prev_log_index <= len(self.ledger.journal)
                else 0
            )
            
            request = {
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries_to_send,
                "leader_commit": self.commit_index
            }
            
            msg = await self.nc.request(
                f"raft.appendentries.{peer}",
                json.dumps(request).encode(),
                timeout=0.5
            )
            response = json.loads(msg.data)
            
            if response.get("term", 0) > self.current_term:
                self.current_term = response["term"]
                self.role = NodeRole.FOLLOWER
                self.voted_for = None
                if self.heartbeat_timer:
                    self.heartbeat_timer.cancel()
                return
            
            if response.get("success"):
                if entries_to_send:
                    self.next_index[peer] = next_idx + len(entries_to_send)
                    self.match_index[peer] = next_idx + len(entries_to_send) - 1
                    await self._update_commit_index()
            else:
                self.next_index[peer] = max(1, self.next_index.get(peer, 1) - 1)
        
        except Exception as e:
            pass
    
    async def _handle_append_entries(self, msg):
        request = json.loads(msg.data)
        
        response = {
            "term": self.current_term,
            "success": False
        }
        
        if request["term"] < self.current_term:
            await msg.respond(json.dumps(response).encode())
            return
        
        if request["term"] > self.current_term:
            self.current_term = request["term"]
            self.voted_for = None
        
        self.role = NodeRole.FOLLOWER
        self.leader_id = request["leader_id"]
        await self._reset_election_timer()
        
        prev_log_index = request["prev_log_index"]
        prev_log_term = request["prev_log_term"]
        
        if prev_log_index > 0:
            if prev_log_index > len(self.ledger.journal):
                await msg.respond(json.dumps(response).encode())
                return
            
            if self.ledger.journal[prev_log_index - 1].epoch != prev_log_term:
                self.ledger.journal = self.ledger.journal[:prev_log_index - 1]
                await msg.respond(json.dumps(response).encode())
                return
        
        for entry_data in request.get("entries", []):
            entry = LedgerEntry(**entry_data)
            existing_idx = entry.seq_no - 1
            if existing_idx < len(self.ledger.journal):
                if self.ledger.journal[existing_idx].epoch != entry.epoch:
                    self.ledger.journal = self.ledger.journal[:existing_idx]
                    entry = self._recompute_entry_hashes(entry)
                    await self.ledger.apply_entry(entry)
            else:
                entry = self._recompute_entry_hashes(entry)
                await self.ledger.apply_entry(entry)
        
        if request["leader_commit"] > self.commit_index:
            self.commit_index = min(
                request["leader_commit"],
                len(self.ledger.journal)
            )
        
        response["success"] = True
        await msg.respond(json.dumps(response).encode())
    
    async def _update_commit_index(self):
        for n in range(len(self.ledger.journal), self.commit_index, -1):
            if self.ledger.journal[n - 1].epoch != self.current_term:
                continue
            
            count = 1
            for peer in self.peers:
                if self.match_index.get(peer, 0) >= n:
                    count += 1
            
            if count > (len(self.peers) + 1) // 2:
                self.commit_index = n
                break
    
    async def handle_client_request(self, msg):
        if self.role != NodeRole.LEADER:
            await msg.respond(json.dumps({
                "error": "not_leader",
                "leader_hint": self.leader_id
            }).encode())
            return
        
        data = json.loads(msg.data)
        idem_key = data.get("idempotency_key")
        
        if idem_key and idem_key in self.idempotency_cache:
            await msg.respond(json.dumps(self.idempotency_cache[idem_key]).encode())
            return
        
        try:
            entry = self._process_operation(data)
            await self.ledger.apply_entry(entry)
            
            tasks = []
            for peer in self.peers:
                task = self._send_append_entries(peer, empty=False)
                tasks.append(task)
            await asyncio.gather(*tasks, return_exceptions=True)
            
            result = {
                "status": "ok",
                "seq_no": entry.seq_no,
                "op_type": entry.op_type.value,
                "account_id": entry.account_id,
                "token": entry.token,
                "amount": str(entry.amount)
            }
            if idem_key:
                self.idempotency_cache[idem_key] = result
            
            await msg.respond(json.dumps(result).encode())
        except Exception as e:
            await msg.respond(json.dumps({"error": str(e)}).encode())
    
    def _process_operation(self, data: dict) -> LedgerEntry:
        op = data["op"]
        
        if op == "lock":
            balance = self.ledger.get_balance(data["account_id"], data["token"])
            amount = Decimal(str(data["amount"]))
            if balance.available < amount:
                raise ValueError("insufficient_funds")
            return self.ledger.create_entry(
                LedgerOpType.LOCK, data["account_id"], data["token"],
                amount, data["ref_id"], self.current_term
            )
        
        elif op == "release":
            if data["ref_id"] not in self.ledger.locked_refs:
                raise ValueError("unknown_ref")
            lock_info = self.ledger.locked_refs[data["ref_id"]]
            return self.ledger.create_entry(
                LedgerOpType.RELEASE, lock_info.account_id, lock_info.token,
                lock_info.amount, data["ref_id"], self.current_term
            )
        
        elif op == "transfer":
            if data["ref_id"] not in self.ledger.locked_refs:
                raise ValueError("unknown_ref")
            lock_info = self.ledger.locked_refs[data["ref_id"]]
            amount = Decimal(str(data["amount"]))
            if amount > lock_info.amount:
                raise ValueError("transfer_amount_exceeds_locked")
            return self.ledger.create_entry(
                LedgerOpType.TRANSFER, data["debit_id"], data["token"],
                amount, data["ref_id"], self.current_term,
                debit_id=data["debit_id"], credit_id=data["credit_id"]
            )
        
        elif op == "commit":
            return self.ledger.create_entry(
                LedgerOpType.COMMIT, data["account_id"], data["token"],
                Decimal(str(data["amount"])), data["ref_id"], self.current_term,
                debit_id=data.get("debit_id"), credit_id=data.get("credit_id")
            )
        
        elif op == "mint":
            return self.ledger.create_entry(
                LedgerOpType.MINT, data["account_id"], data["token"],
                Decimal(str(data["amount"])), data.get("ref_id", "mint"), self.current_term
            )
        
        raise ValueError("unknown_operation")
    
    async def handle_replication(self, msg):
        if self.role == NodeRole.LEADER:
            return
        entry_data = json.loads(msg.data)
        if entry_data["epoch"] < self.current_term:
            return
        entry = LedgerEntry(**entry_data)
        entry = self._recompute_entry_hashes(entry)
        await self.ledger.apply_entry(entry)
    
    async def handle_resync(self, msg):
        if self.role != NodeRole.LEADER:
            return
        data = json.loads(msg.data)
        last_seq = data.get("last_seq", 0)
        entries = []
        for e in self.ledger.journal:
            if e.seq_no > last_seq:
                entry_dict = e.model_dump()
                entry_dict['amount'] = str(entry_dict['amount'])
                entries.append(entry_dict)
        await msg.respond(json.dumps({"entries": entries}).encode())
    
    async def resync_from_leader(self):
        if self.role == NodeRole.LEADER:
            return
        
        last_seq = self.ledger.journal[-1].seq_no if self.ledger.journal else 0
        
        for attempt in range(5):
            try:
                if not self.leader_id:
                    await asyncio.sleep(1)
                    continue
                
                msg = await self.nc.request(
                    f"raft.resync.{self.leader_id}",
                    json.dumps({"last_seq": last_seq}).encode(),
                    timeout=5.0
                )
                response = json.loads(msg.data)
                entries = response.get("entries", [])
                
                if entries:
                    for entry_data in entries:
                        entry = LedgerEntry(**entry_data)
                        if entry.seq_no > last_seq:
                            entry = self._recompute_entry_hashes(entry)
                            await self.ledger.apply_entry(entry)
                            last_seq = entry.seq_no
                    print(f"[{self.node_id}] Resync successful: applied {len(entries)} entries")
                    return
                else:
                    return
            
            except Exception as e:
                if attempt < 4:
                    await asyncio.sleep(2)
    
    async def _periodic_resync(self):
        while self.running:
            await asyncio.sleep(5)
            if self.role != NodeRole.LEADER and self.leader_id:
                last_seq = self.ledger.journal[-1].seq_no if self.ledger.journal else 0
                try:
                    msg = await self.nc.request(
                        f"raft.resync.{self.leader_id}",
                        json.dumps({"last_seq": last_seq}).encode(),
                        timeout=5.0
                    )
                    response = json.loads(msg.data)
                    entries = response.get("entries", [])
                    
                    if entries:
                        for entry_data in entries:
                            entry = LedgerEntry(**entry_data)
                            if entry.seq_no > last_seq:
                                entry = self._recompute_entry_hashes(entry)
                                await self.ledger.apply_entry(entry)
                                last_seq = entry.seq_no
                except Exception:
                    pass
    
    def get_status(self) -> dict:
        return {
            "role": self.role.value,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "commit_index": self.commit_index
        }