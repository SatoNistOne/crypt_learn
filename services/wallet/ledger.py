import os
from decimal import Decimal
from dataclasses import dataclass, field
from typing import Optional
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy import select, text
from shared.schemas import LedgerEntry, LedgerOpType
from services.wallet.hash_chain import compute_hash, create_entry_payload

class Base(DeclarativeBase):
    pass

def make_journal_model(table_name: str):
    class JournalEntryModel(Base):
        __tablename__ = table_name
        
        seq_no: Mapped[int] = mapped_column(primary_key=True)
        op_type: Mapped[str] = mapped_column()
        account_id: Mapped[str] = mapped_column()
        token: Mapped[str] = mapped_column()
        amount: Mapped[str] = mapped_column()
        ref_id: Mapped[str] = mapped_column()
        prev_hash: Mapped[str] = mapped_column()
        this_hash: Mapped[str] = mapped_column()
        epoch: Mapped[int] = mapped_column()
        debit_id: Mapped[Optional[str]] = mapped_column(nullable=True)
        credit_id: Mapped[Optional[str]] = mapped_column(nullable=True)
    
    return JournalEntryModel

class OrderModel(Base):
    __tablename__ = 'orders'
    
    order_id: Mapped[str] = mapped_column(primary_key=True)
    account_id: Mapped[str] = mapped_column()
    pair: Mapped[str] = mapped_column()
    side: Mapped[str] = mapped_column()
    price: Mapped[str] = mapped_column()
    quantity: Mapped[str] = mapped_column()
    filled_quantity: Mapped[str] = mapped_column(default="0")
    status: Mapped[str] = mapped_column(default="OPEN")
    seq_no: Mapped[Optional[int]] = mapped_column(nullable=True)
    created_at: Mapped[str] = mapped_column()

class TradeModel(Base):
    __tablename__ = 'trades'
    
    trade_id: Mapped[str] = mapped_column(primary_key=True)
    pair: Mapped[str] = mapped_column()
    buyer_id: Mapped[str] = mapped_column()
    seller_id: Mapped[str] = mapped_column()
    buy_order_id: Mapped[str] = mapped_column()
    sell_order_id: Mapped[str] = mapped_column()
    price: Mapped[str] = mapped_column()
    quantity: Mapped[str] = mapped_column()
    created_at: Mapped[str] = mapped_column()

@dataclass
class Balance:
    available: Decimal = field(default_factory=lambda: Decimal("0"))
    locked: Decimal = field(default_factory=lambda: Decimal("0"))

@dataclass
class LockInfo:
    account_id: str
    token: str
    amount: Decimal
    original_amount: Decimal

class Ledger:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.balances: dict[str, dict[str, Balance]] = {}
        self.locked_refs: dict[str, LockInfo] = {}
        self.journal: list[LedgerEntry] = []
        
        self.journal_table_name = f"journal_{node_id.replace('-', '_')}"
        self.JournalEntryModel = make_journal_model(self.journal_table_name)
        
        db_url = os.getenv("DATABASE_URL", f"sqlite+aiosqlite:///{node_id}.db")
        self.engine = create_async_engine(db_url, echo=False)
        self.async_session = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
    
    async def init_db(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        async with self.engine.begin() as conn:
            await conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.journal_table_name}_account 
                ON {self.journal_table_name}(account_id)
            """))
            await conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.journal_table_name}_token 
                ON {self.journal_table_name}(token)
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_orders_account ON orders(account_id)
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trades_pair ON trades(pair)
            """))
        
        await self._load_from_db()
    
    async def _load_from_db(self):
        async with self.async_session() as session:
            result = await session.execute(
                select(self.JournalEntryModel).order_by(self.JournalEntryModel.seq_no)
            )
            db_entries = result.scalars().all()
            
            for db_entry in db_entries:
                entry = LedgerEntry(
                    seq_no=db_entry.seq_no,
                    op_type=LedgerOpType(db_entry.op_type),
                    account_id=db_entry.account_id,
                    token=db_entry.token,
                    amount=Decimal(db_entry.amount),
                    ref_id=db_entry.ref_id,
                    prev_hash=db_entry.prev_hash,
                    this_hash=db_entry.this_hash,
                    epoch=db_entry.epoch,
                    debit_id=db_entry.debit_id,
                    credit_id=db_entry.credit_id
                )
                self.journal.append(entry)
                self._apply_to_balances(entry)
            
            print(f"[{self.node_id}] Loaded {len(self.journal)} entries from database")
    
    def _apply_to_balances(self, entry: LedgerEntry):
        if entry.account_id not in self.balances:
            self.balances[entry.account_id] = {}
        if entry.token not in self.balances[entry.account_id]:
            self.balances[entry.account_id][entry.token] = Balance()
        
        if entry.op_type == LedgerOpType.MINT:
            self.balances[entry.account_id][entry.token].available += entry.amount
        
        elif entry.op_type == LedgerOpType.LOCK:
            self.balances[entry.account_id][entry.token].available -= entry.amount
            self.balances[entry.account_id][entry.token].locked += entry.amount
            self.locked_refs[entry.ref_id] = LockInfo(
                entry.account_id, entry.token, entry.amount, entry.amount
            )
        
        elif entry.op_type == LedgerOpType.RELEASE:
            lock_info = self.locked_refs.pop(entry.ref_id, None)
            if lock_info:
                if lock_info.account_id not in self.balances:
                    self.balances[lock_info.account_id] = {}
                if lock_info.token not in self.balances[lock_info.account_id]:
                    self.balances[lock_info.account_id][lock_info.token] = Balance()
                self.balances[lock_info.account_id][lock_info.token].locked -= lock_info.amount
                self.balances[lock_info.account_id][lock_info.token].available += lock_info.amount
        
        elif entry.op_type == LedgerOpType.TRANSFER:
            if entry.debit_id and entry.credit_id:
                if entry.debit_id not in self.balances:
                    self.balances[entry.debit_id] = {}
                if entry.token not in self.balances[entry.debit_id]:
                    self.balances[entry.debit_id][entry.token] = Balance()
                if entry.credit_id not in self.balances:
                    self.balances[entry.credit_id] = {}
                if entry.token not in self.balances[entry.credit_id]:
                    self.balances[entry.credit_id][entry.token] = Balance()
                
                lock_info = self.locked_refs.get(entry.ref_id)
                if lock_info:
                    if lock_info.amount < entry.amount:
                        raise ValueError("transfer_amount_exceeds_locked")
                    
                    self.balances[entry.debit_id][entry.token].locked -= entry.amount
                    self.balances[entry.credit_id][entry.token].available += entry.amount
                    
                    lock_info.amount -= entry.amount
                    if lock_info.amount <= 0:
                        self.locked_refs.pop(entry.ref_id, None)
        
        elif entry.op_type == LedgerOpType.COMMIT:
            if entry.debit_id and entry.credit_id:
                if entry.debit_id not in self.balances:
                    self.balances[entry.debit_id] = {}
                if entry.token not in self.balances[entry.debit_id]:
                    self.balances[entry.debit_id][entry.token] = Balance()
                if entry.credit_id not in self.balances:
                    self.balances[entry.credit_id] = {}
                if entry.token not in self.balances[entry.credit_id]:
                    self.balances[entry.credit_id][entry.token] = Balance()
                
                self.balances[entry.debit_id][entry.token].locked -= entry.amount
                self.balances[entry.credit_id][entry.token].available += entry.amount
    
    def get_balance(self, account_id: str, token: str) -> Balance:
        return self.balances.get(account_id, {}).get(token, Balance())
    
    def get_all_balances(self, account_id: str) -> dict[str, Balance]:
        return self.balances.get(account_id, {})
    
    async def apply_entry(self, entry: LedgerEntry):
        self._apply_to_balances(entry)
        self.journal.append(entry)
        await self._save_to_db(entry)
    
    async def _save_to_db(self, entry: LedgerEntry):
        async with self.async_session() as session:
            db_entry = self.JournalEntryModel(
                seq_no=entry.seq_no,
                op_type=entry.op_type.value,
                account_id=entry.account_id,
                token=entry.token,
                amount=str(entry.amount),
                ref_id=entry.ref_id,
                prev_hash=entry.prev_hash,
                this_hash=entry.this_hash,
                epoch=entry.epoch,
                debit_id=entry.debit_id,
                credit_id=entry.credit_id
            )
            session.add(db_entry)
            await session.commit()
    
    async def save_order(self, order_id: str, account_id: str, pair: str, 
                         side: str, price: Decimal, quantity: Decimal, seq_no: int = 0):
        from datetime import datetime
        async with self.async_session() as session:
            order = OrderModel(
                order_id=order_id,
                account_id=account_id,
                pair=pair,
                side=side,
                price=str(price),
                quantity=str(quantity),
                filled_quantity="0",
                status="OPEN",
                seq_no=seq_no,
                created_at=datetime.utcnow().isoformat()
            )
            session.add(order)
            await session.commit()
    
    async def update_order_status(self, order_id: str, status: str, filled_quantity: Decimal):
        async with self.async_session() as session:
            result = await session.execute(
                select(OrderModel).where(OrderModel.order_id == order_id)
            )
            order = result.scalar_one_or_none()
            if order:
                order.status = status
                order.filled_quantity = str(filled_quantity)
                await session.commit()
    
    async def save_trade(self, trade_id: str, pair: str, buyer_id: str, seller_id: str,
                        buy_order_id: str, sell_order_id: str, price: Decimal, quantity: Decimal):
        from datetime import datetime
        async with self.async_session() as session:
            trade = TradeModel(
                trade_id=trade_id,
                pair=pair,
                buyer_id=buyer_id,
                seller_id=seller_id,
                buy_order_id=buy_order_id,
                sell_order_id=sell_order_id,
                price=str(price),
                quantity=str(quantity),
                created_at=datetime.utcnow().isoformat()
            )
            session.add(trade)
            await session.commit()
    
    def create_entry(self, op_type: LedgerOpType, account_id: str, token: str,
                     amount: Decimal, ref_id: str, epoch: int,
                     debit_id: Optional[str] = None,
                     credit_id: Optional[str] = None) -> LedgerEntry:
        prev_hash = self.journal[-1].this_hash if self.journal else "0" * 64
        seq_no = len(self.journal) + 1
        
        entry = LedgerEntry(
            seq_no=seq_no, op_type=op_type, account_id=account_id,
            token=token, amount=amount, ref_id=ref_id,
            prev_hash=prev_hash, this_hash="", epoch=epoch,
            debit_id=debit_id, credit_id=credit_id
        )
        payload = create_entry_payload(entry)
        entry.this_hash = compute_hash(prev_hash, payload)
        return entry