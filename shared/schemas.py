from pydantic import BaseModel
from decimal import Decimal
from enum import Enum
from typing import Optional

class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderStatus(str, Enum):
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"

class LedgerOpType(str, Enum):
    MINT = "MINT"
    LOCK = "LOCK"
    RELEASE = "RELEASE"
    TRANSFER = "TRANSFER"
    COMMIT = "COMMIT"

class LedgerEntry(BaseModel):
    seq_no: int
    op_type: LedgerOpType
    account_id: str
    token: str
    amount: Decimal
    ref_id: str
    prev_hash: str
    this_hash: str
    epoch: int
    debit_id: Optional[str] = None
    credit_id: Optional[str] = None

class Order(BaseModel):
    order_id: str
    account_id: str
    pair: str
    side: OrderSide
    price: Decimal
    quantity: Decimal
    seq_no: int = 0
    status: OrderStatus = OrderStatus.OPEN
    filled_quantity: Decimal = Decimal("0")

class Trade(BaseModel):
    trade_id: str
    pair: str
    buyer_id: str
    seller_id: str
    buy_order_id: str
    sell_order_id: str
    price: Decimal
    quantity: Decimal

class StatusResponse(BaseModel):
    role: str
    term: int
    epoch: int
    last_seq: int
    leader_id: Optional[str] = None

class UserRegister(BaseModel):
    username: str
    password: str

class UserLogin(BaseModel):
    username: str
    password: str

class UserResponse(BaseModel):
    username: str
    token: str