import json
import hashlib
from shared.schemas import LedgerEntry

def canonical_json(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)

def compute_hash(prev_hash: str, payload: dict) -> str:
    return hashlib.sha256((prev_hash + canonical_json(payload)).encode()).hexdigest()

def create_entry_payload(entry: LedgerEntry) -> dict:
    payload = {
        "op": entry.op_type.value,
        "acc": entry.account_id,
        "token": entry.token,
        "amt": str(entry.amount),
        "ref": entry.ref_id,
        "seq": entry.seq_no
    }
    if entry.debit_id:
        payload["debit"] = entry.debit_id
    if entry.credit_id:
        payload["credit"] = entry.credit_id
    return payload

def verify_chain(entries: list[LedgerEntry]) -> tuple[bool, int]:
    prev_hash = "0" * 64
    for i, entry in enumerate(entries):
        payload = create_entry_payload(entry)
        expected_hash = compute_hash(prev_hash, payload)
        if expected_hash != entry.this_hash:
            return False, i
        prev_hash = entry.this_hash
    return True, -1