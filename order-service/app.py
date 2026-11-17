import asyncio
import logging
import os
import random
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

SERVICE_NAME = "order-service"
PORT = 5001
DB_PATH = os.environ.get("DB_PATH", "/app/data/orders.db")
USER_SERVICE_URL = os.environ.get("USER_SERVICE_URL", "http://user-service:5000")
NOTIFICATION_SERVICE_URL = os.environ.get("NOTIFICATION_SERVICE_URL", "http://notification-service:5002")

HTTP_TIMEOUT = httpx.Timeout(2.0)
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 0.3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger(SERVICE_NAME)


class ProductCreate(BaseModel):
    name: str = Field(min_length=1, description="название товара")
    description: str = Field(default="", description="описание товара")
    price: float = Field(gt=0, description="цена, должна быть положительной")


class OrderCreate(BaseModel):
    product_id: int = Field(description="id товара из каталога Order Service")
    user_id: int = Field(description="id пользователя из User Service")


http_client: httpx.AsyncClient | None = None


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
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                price REAL NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                product_name TEXT NOT NULL,
                price REAL NOT NULL,
                user_name TEXT NOT NULL,
                user_email TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )


def jitter_backoff(attempt: int) -> float:
    delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
    return delay + random.uniform(0, delay)


async def request_with_retry(method: str, url: str, *, json_body=None):
    last_error: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = await http_client.request(method, url, json=json_body, timeout=HTTP_TIMEOUT)
            if resp.status_code >= 500:
                raise httpx.HTTPStatusError(
                    f"upstream returned HTTP {resp.status_code}",
                    request=resp.request,
                    response=resp,
                )
            return resp
        except httpx.HTTPError as exc:
            last_error = exc

        if attempt < RETRY_ATTEMPTS:
            delay = jitter_backoff(attempt)
            log.warning(
                "RETRY %s %s failed (%s: %s); attempt %d/%d, retry in %.2fs",
                method, url, type(last_error).__name__, last_error, attempt, RETRY_ATTEMPTS, delay,
            )
            await asyncio.sleep(delay)

    raise last_error


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient()
    init_db()
    yield
    await http_client.aclose()


app = FastAPI(title="Order Service", lifespan=lifespan)


@app.post("/products", status_code=201)
async def create_product(body: ProductCreate):
    name = body.name.strip()
    description = body.description.strip()

    created_at = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.execute(
            "INSERT INTO products (name, description, price, created_at) VALUES (?, ?, ?, ?)",
            (name, description, body.price, created_at),
        )
        product_id = cur.lastrowid

    log.info("PRODUCT_CREATED id=%s name=%r price=%s", product_id, name, body.price)
    return {
        "id": product_id,
        "name": name,
        "description": description,
        "price": body.price,
        "created_at": created_at,
    }


@app.get("/products")
async def list_products():
    with db() as conn:
        rows = conn.execute("SELECT id, name, description, price, created_at FROM products ORDER BY id").fetchall()

    items = [dict(row) for row in rows]
    return JSONResponse({"items": items, "count": len(items)})


@app.post("/orders", status_code=201)
async def create_order(body: OrderCreate):
    product_id = body.product_id
    user_id = body.user_id
    with db() as conn:
        product = conn.execute("SELECT id, name, price FROM products WHERE id = ?", (product_id,)).fetchone()
    if product is None:
        log.warning("ORDER_REJECTED product_not_found product_id=%d", product_id)
        return JSONResponse({"error": f"product {product_id} not found"}, status_code=404)

    try:
        resp = await request_with_retry("GET", f"{USER_SERVICE_URL}/users/{user_id}")
    except httpx.HTTPError as exc:
        log.error(
            "USER_SERVICE_UNAVAILABLE user_id=%d error=%s: %s -> order rejected",
            user_id, type(exc).__name__, exc,
        )
        return JSONResponse(
            {"error": f"user-service is unreachable after {RETRY_ATTEMPTS} attempts, order rejected"},
            status_code=503,
        )
    if resp.status_code == 404:
        log.warning("ORDER_REJECTED user_not_found user_id=%d", user_id)
        return JSONResponse({"error": f"user {user_id} not found"}, status_code=404)
    if resp.status_code != 200:
        log.error("USER_SERVICE_UNEXPECTED_STATUS status=%d user_id=%d", resp.status_code, user_id)
        return JSONResponse({"error": f"user-service returned unexpected status {resp.status_code}"}, status_code=502)

    user = resp.json()
    user_name = user.get("name")
    user_email = user.get("email")
    log.info("USER_CHECK_OK user_id=%d name=%r email=%r", user_id, user_name, user_email)

    created_at = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO orders (product_id, user_id, product_name, price, user_name, user_email, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (product_id, user_id, product["name"], product["price"], user_name, user_email, created_at),
        )
        order_id = cur.lastrowid
    log.info(
        "ORDER_CREATED id=%d product_id=%d user_id=%d price=%s",
        order_id, product_id, user_id, product["price"],
    )

    notification_status = "sent"
    try:
        notif_resp = await request_with_retry(
            "POST",
            f"{NOTIFICATION_SERVICE_URL}/notify",
            json_body={
                "order_id": order_id,
                "user_name": user_name,
                "user_email": user_email,
                "product_name": product["name"],
            },
        )
        if 200 <= notif_resp.status_code < 300:
            log.info("NOTIFICATION_SENT order_id=%d", order_id)
        else:
            notification_status = "failed"
            log.error(
                "NOTIFICATION_FAILED order_id=%d unexpected_status=%d",
                order_id, notif_resp.status_code,
            )
    except httpx.HTTPError as exc:
        notification_status = "failed"
        log.error(
            "NOTIFICATION_FAILED order_id=%d error=%s: %s (order remains created)",
            order_id, type(exc).__name__, exc,
        )

    return {
        "id": order_id,
        "product_id": product_id,
        "user_id": user_id,
        "product_name": product["name"],
        "price": product["price"],
        "user_name": user_name,
        "user_email": user_email,
        "notification": notification_status,
        "created_at": created_at,
    }


@app.get("/orders")
async def list_orders():
    with db() as conn:
        rows = conn.execute(
            """
            SELECT id, product_id, user_id, product_name, price, user_name, user_email, created_at
            FROM orders ORDER BY id
            """
        ).fetchall()

    items = [dict(row) for row in rows]
    return JSONResponse({"items": items, "count": len(items)})


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}
