from fastapi import APIRouter, HTTPException, Header, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from decimal import Decimal
from services.api_gateway.saga import SagaOrchestrator
from services.api_gateway.auth import AuthManager

class PlaceOrderRequest(BaseModel):
    pair: str
    side: str
    price: float
    quantity: float

def create_routes(saga: SagaOrchestrator, auth_manager: AuthManager):
    router = APIRouter()
    security = HTTPBearer()
    
    async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
        user = auth_manager.get_user(credentials.credentials)
        if not user:
            raise HTTPException(status_code=401, detail="Invalid token")
        return user
    
    @router.post("/orders")
    async def place_order(req: PlaceOrderRequest, user: str = Depends(get_current_user)):
        result = await saga.place_order(
            account_id=user, pair=req.pair, side=req.side,
            price=Decimal(str(req.price)), quantity=Decimal(str(req.quantity))
        )
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        return result
    
    @router.delete("/orders/{order_id}")
    async def cancel_order(order_id: str, pair: str, user: str = Depends(get_current_user)):
        result = await saga.cancel_order(order_id, pair)
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        return result
    
    return router