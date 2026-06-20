from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from nats.aio.client import Client as NATS
from shared.config import Config
from shared.schemas import UserRegister, UserLogin, UserResponse
from services.api_gateway.saga import SagaOrchestrator
from services.api_gateway.handlers import TradeHandler
from services.api_gateway.routes import create_routes
from services.api_gateway.auth import AuthManager
import httpx

app = FastAPI()
config = Config()
nc = NATS()
saga = SagaOrchestrator(nc)
trade_handler = TradeHandler(nc, saga)
auth_manager = AuthManager()

app.include_router(create_routes(saga, auth_manager))

MATCHING_ENGINES = {
    "BTC_LEARN": "http://matching-engine-btc:8000",
    "ETH_LEARN": "http://matching-engine-eth:8000",
}

@app.on_event("startup")
async def startup():
    await auth_manager.init_db()
    await nc.connect(config.nats_url)
    await nc.subscribe("trades.executed.>", cb=trade_handler.handle_trade)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/auth/register")
async def register(user: UserRegister):
    result = await auth_manager.register(user.username, user.password)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@app.post("/auth/login")
async def login(user: UserLogin):
    result = await auth_manager.login(user.username, user.password)
    if "error" in result:
        raise HTTPException(status_code=401, detail=result["error"])
    return result

@app.post("/auth/logout")
async def logout(credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())):
    result = auth_manager.logout(credentials.credentials)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@app.get("/auth/me")
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())):
    user = auth_manager.get_user(credentials.credentials)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"username": user}

@app.get("/balances/{user}")
async def get_balances(user: str):
    async with httpx.AsyncClient() as client:
        for wallet_url in ["http://wallet-1:8000", "http://wallet-2:8000", "http://wallet-3:8000"]:
            try:
                resp = await client.get(f"{wallet_url}/balances/{user}", timeout=2.0)
                if resp.status_code == 200:
                    return resp.json()
            except:
                continue
        return {"error": "all_wallets_unavailable"}

@app.get("/orderbook/{pair}")
async def get_orderbook(pair: str, depth: int = 10):
    me_url = MATCHING_ENGINES.get(pair)
    if not me_url:
        return {"error": f"unknown_pair_{pair}"}
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{me_url}/orderbook?depth={depth}", timeout=2.0)
            return resp.json()
        except Exception as e:
            return {"error": str(e)}

@app.get("/trades/{pair}")
async def get_trades(pair: str, limit: int = 50):
    me_url = MATCHING_ENGINES.get(pair)
    if not me_url:
        return {"error": f"unknown_pair_{pair}"}
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{me_url}/trades?limit={limit}", timeout=2.0)
            return resp.json()
        except Exception as e:
            return {"error": str(e)}

@app.get("/status/wallet")
async def wallet_status():
    async with httpx.AsyncClient() as client:
        for wallet_url in ["http://wallet-1:8000", "http://wallet-2:8000", "http://wallet-3:8000"]:
            try:
                resp = await client.get(f"{wallet_url}/status", timeout=2.0)
                if resp.status_code == 200:
                    return resp.json()
            except:
                continue
        return {"error": "all_wallets_unavailable"}

@app.get("/status/matching")
async def matching_status():
    result = {}
    async with httpx.AsyncClient() as client:
        for pair, url in MATCHING_ENGINES.items():
            try:
                resp = await client.get(f"{url}/status", timeout=2.0)
                if resp.status_code == 200:
                    result[pair] = resp.json()
            except Exception as e:
                result[pair] = {"error": str(e)}
    return result

app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")