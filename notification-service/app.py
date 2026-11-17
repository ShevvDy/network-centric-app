import logging
import os
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel, Field

SERVICE_NAME = "notification-service"
PORT = 5002
DB_PATH = os.environ.get("DB_PATH", "/app/data/notifications.db")

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger(SERVICE_NAME)


class NotifyRequest(BaseModel):
    order_id: int
    user_name: str = Field(min_length=1, description="ФИО пользователя")
    user_email: str = Field(pattern=EMAIL_PATTERN, description="электронная почта")
    product_name: str = Field(min_length=1, description="название товара")


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
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                user_email TEXT NOT NULL,
                product_name TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Notification Service", lifespan=lifespan)


@app.post("/notify", status_code=201)
async def notify(body: NotifyRequest):
    user_name = body.user_name.strip()
    user_email = body.user_email.strip()
    product_name = body.product_name.strip()

    message = (
        f"Пользователь {user_name} ({user_email}) сделал заказ товара {product_name}, номер заказа: {body.order_id}"
    )

    created_at = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO notifications (order_id, user_name, user_email, product_name, message, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (body.order_id, user_name, user_email, product_name, message, created_at),
        )
        notification_id = cur.lastrowid

    log.info("NOTIFICATION id=%d order_id=%d: %s", notification_id, body.order_id, message)

    return {"status": "sent", "notification_id": notification_id, "message": message}


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}
