import os
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, Header, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select, text
from pydantic import BaseModel
import httpx

DATABASE_URL = os.getenv("AUTH_DATABASE_URL", "postgresql+asyncpg://cryptlearn:cryptlearn123@postgres/cryptlearn")
FRONTEND_DIR = os.getenv("FRONTEND_DIR", "./frontend")

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"
    username: Mapped[str] = mapped_column(primary_key=True)
    password_hash: Mapped[str] = mapped_column()
    active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[str] = mapped_column(default_factory=lambda: datetime.utcnow().isoformat())

class SessionModel(Base):
    __tablename__ = "sessions"
    token: Mapped[str] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column()
    created_at: Mapped[str] = mapped_column(default_factory=lambda: datetime.utcnow().isoformat())
    expires_at: Mapped[str] = mapped_column()

class RegisterRequest(BaseModel):
    username: str
    password: str

class LoginRequest(BaseModel):
    username: str
    password: str

class AuthResponse(BaseModel):
    username: str
    token: str

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def generate_token() -> str:
    return secrets.token_urlsafe(32)

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

async def get_current_user(authorization: Optional[str] = Header(None), db: AsyncSession = Depends(get_db)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing authorization header")
    token = authorization.replace("Bearer ", "")
    result = await db.execute(select(SessionModel).where(SessionModel.token == token))
    session_obj = result.scalar_one_or_none()
    if not session_obj:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if datetime.fromisoformat(session_obj.expires_at) < datetime.utcnow():
        raise HTTPException(status_code=401, detail="Token expired")
    result = await db.execute(select(User).where(User.username == session_obj.username))
    user = result.scalar_one_or_none()
    if not user or not user.active:
        raise HTTPException(status_code=401, detail="User not found or inactive")
    return user

async def add_initial_balance(username: str):
    try:
        initial_balances = {'BTC': '10', 'ETH': '100', 'LEARN': '10000'}
        table_name = "journal_wallet_3"
        async with engine.begin() as conn:
            result = await conn.execute(text(f"SELECT COALESCE(MAX(seq_no), 0) FROM {table_name}"))
            max_seq = result.scalar()
            prev_hash = '0' * 64
            if max_seq > 0:
                result = await conn.execute(text(f"SELECT this_hash FROM {table_name} WHERE seq_no = :seq"), {"seq": max_seq})
                prev_hash = result.scalar() or '0' * 64
            for token, amount in initial_balances.items():
                max_seq += 1
                payload = f"{max_seq}|MINT|{username}|{token}|{amount}|initial_{username}_{token}|{prev_hash}|1"
                this_hash = hashlib.sha256((prev_hash + payload).encode()).hexdigest()
                await conn.execute(text(f"""
                    INSERT INTO {table_name} (seq_no, op_type, account_id, token, amount, ref_id, prev_hash, this_hash, epoch)
                    VALUES (:seq, 'MINT', :user, :token, :amount, :ref, :prev, :this, 1)
                """), {"seq": max_seq, "user": username, "token": token, "amount": amount, "ref": f"initial_{username}_{token}", "prev": prev_hash, "this": this_hash})
                prev_hash = this_hash
    except Exception as e:
        print(f"Failed to add initial balance: {e}")

app = FastAPI(title="CryptLearn API Gateway")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if os.path.exists(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@app.post("/auth/register", response_model=AuthResponse)
async def register(req: RegisterRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.username == req.username))
    if result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Username already exists")
    user = User(username=req.username, password_hash=hash_password(req.password))
    db.add(user)
    await db.commit()
    await db.refresh(user)
    token = generate_token()
    session = SessionModel(token=token, username=user.username, expires_at=(datetime.utcnow() + timedelta(days=7)).isoformat())
    db.add(session)
    await db.commit()
    await add_initial_balance(user.username)
    return AuthResponse(username=user.username, token=token)

@app.post("/auth/login", response_model=AuthResponse)
async def login(req: LoginRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.username == req.username))
    user = result.scalar_one_or_none()
    if not user or user.password_hash != hash_password(req.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = generate_token()
    session = SessionModel(token=token, username=user.username, expires_at=(datetime.utcnow() + timedelta(days=7)).isoformat())
    db.add(session)
    await db.commit()
    return AuthResponse(username=user.username, token=token)

@app.post("/auth/logout")
async def logout(db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    result = await db.execute(select(SessionModel).where(SessionModel.username == user.username))
    sessions = result.scalars().all()
    for s in sessions:
        await db.delete(s)
    await db.commit()
    return {"status": "ok"}

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/balances/{user}")
async def get_balances(user: str):
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"http://wallet-3:8000/balances/{user}")
        return resp.json()

@app.post("/orders")
async def place_order(req: Request, user: User = Depends(get_current_user)):
    body = await req.json()
    async with httpx.AsyncClient() as client:
        resp = await client.post("http://matching-engine-btc:8000/orders", json=body)
        return resp.json()

@app.delete("/orders/{order_id}")
async def cancel_order(order_id: str, pair: str, user: User = Depends(get_current_user)):
    async with httpx.AsyncClient() as client:
        resp = await client.delete(f"http://matching-engine-btc:8000/orders/{order_id}", params={"pair": pair})
        return resp.json()

@app.get("/orderbook/{pair}")
async def get_orderbook(pair: str):
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"http://matching-engine-btc:8000/orderbook/{pair}")
        return resp.json()

@app.get("/trades/{pair}")
async def get_trades(pair: str):
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"http://matching-engine-btc:8000/trades/{pair}")
        return resp.json()

@app.get("/status/wallet")
async def get_wallet_status():
    async with httpx.AsyncClient() as client:
        resp = await client.get("http://wallet-3:8000/status")
        return resp.json()

@app.get("/status/matching")
async def get_matching_status():
    return {"BTC_LEARN": {"orders_count": 0, "trades_count": 0}, "ETH_LEARN": {"orders_count": 0, "trades_count": 0}}