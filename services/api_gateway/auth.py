import os
import hashlib
import secrets
from typing import Optional
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy import select, text

class Base(DeclarativeBase):
    pass

class UserModel(Base):
    __tablename__ = 'users'
    
    username: Mapped[str] = mapped_column(primary_key=True)
    password_hash: Mapped[str] = mapped_column()
    active: Mapped[int] = mapped_column(default=1)
    created_at: Mapped[str] = mapped_column()

class SessionModel(Base):
    __tablename__ = 'sessions'
    
    token: Mapped[str] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column()
    created_at: Mapped[str] = mapped_column()
    expires_at: Mapped[Optional[str]] = mapped_column(nullable=True)

class AuthManager:
    def __init__(self):
        self.tokens: dict[str, str] = {}
        
        db_url = os.getenv("AUTH_DATABASE_URL", "postgresql+asyncpg://cryptlearn:cryptlearn123@postgres/cryptlearn")
        self.engine = create_async_engine(db_url, echo=False)
        self.async_session = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
    
    async def init_db(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        async with self.engine.begin() as conn:
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_sessions_username ON sessions(username)
            """))
        
        print("[auth] Database initialized")
    
    async def register(self, username: str, password: str) -> dict:
        async with self.async_session() as session:
            result = await session.execute(
                select(UserModel).where(UserModel.username == username)
            )
            if result.scalar_one_or_none():
                return {"error": "User already exists"}
            
            password_hash = hashlib.sha256(password.encode()).hexdigest()
            new_user = UserModel(
                username=username,
                password_hash=password_hash,
                created_at=datetime.utcnow().isoformat()
            )
            session.add(new_user)
            await session.commit()
            
            token = self._generate_token(username)
            return {"username": username, "token": token}
    
    async def login(self, username: str, password: str) -> dict:
        async with self.async_session() as session:
            result = await session.execute(
                select(UserModel).where(UserModel.username == username)
            )
            user = result.scalar_one_or_none()
            
            if not user:
                return {"error": "User not found"}
            
            password_hash = hashlib.sha256(password.encode()).hexdigest()
            if user.password_hash != password_hash:
                return {"error": "Invalid password"}
            
            token = self._generate_token(username)
            return {"username": username, "token": token}
    
    def logout(self, token: str) -> dict:
        if token in self.tokens:
            del self.tokens[token]
            return {"status": "logged_out"}
        return {"error": "Invalid token"}
    
    def get_user(self, token: str) -> Optional[str]:
        return self.tokens.get(token)
    
    def _generate_token(self, username: str) -> str:
        token = secrets.token_urlsafe(32)
        self.tokens[token] = username
        return token