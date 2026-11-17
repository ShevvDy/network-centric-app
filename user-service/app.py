import logging
import os
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

SERVICE_NAME = "user-service"
PORT = 5000
DB_PATH = os.environ.get("DB_PATH", "/app/data/users.db")

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger(SERVICE_NAME)


class UserCreate(BaseModel):
    name: str = Field(min_length=1, description="ФИО пользователя")
    email: str = Field(pattern=EMAIL_PATTERN, description="электронная почта")


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(title="User Service", lifespan=lifespan)


@app.post("/users", status_code=201)
async def register_user(body: UserCreate):
    name = body.name.strip()
    email = body.email.strip()

    created_at = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.execute(
            "INSERT INTO users (name, email, created_at) VALUES (?, ?, ?)",
            (name, email, created_at),
        )
        user_id = cur.lastrowid

    log.info("USER_CREATED id=%s name=%r email=%r", user_id, name, email)
    return {"id": user_id, "name": name, "email": email, "created_at": created_at}


@app.get("/users/{user_id}")
async def get_user(user_id: int):
    with db() as conn:
        row = conn.execute(
            "SELECT id, name, email, created_at FROM users WHERE id = ?", (user_id,)
        ).fetchone()

    if row is None:
        log.warning("USER_NOT_FOUND id=%d", user_id)
        return JSONResponse({"error": f"user {user_id} not found"}, status_code=404)

    log.info("USER_FETCHED id=%d", user_id)
    return JSONResponse(dict(row))


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}
