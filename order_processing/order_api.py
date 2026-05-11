# order_processing/order_api.py
# FastAPI Order Microservice — nhận đơn, đẩy queue, query DB thật
# Chạy: uvicorn order_api:app --port 8000 --reload

import os
import json
import logging
from typing import Optional

import pika
import pymysql
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="MicroSync Order API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Pydantic Models ──────────────────────────────────────────────────────────

class OrderCreate(BaseModel):
    user_id:    int   = Field(..., gt=0, description="ID khách hàng")
    product_id: int   = Field(..., gt=0, description="ID sản phẩm")
    quantity:   int   = Field(..., gt=0, le=1000, description="Số lượng (1-1000)")
    total_price: float = Field(..., gt=0, description="Tổng giá trị")

# ─── DB helpers ───────────────────────────────────────────────────────────────

def get_mysql():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3307)),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASS", "root"),
        database=os.getenv("MYSQL_DB", "order_db"),
        cursorclass=pymysql.cursors.DictCursor,
    )

# ─── Queue helper ─────────────────────────────────────────────────────────────

def publish_to_queue(order_data: dict):
    """Đẩy event đơn hàng vào RabbitMQ."""
    try:
        credentials = pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "admin"),
            os.getenv("RABBITMQ_PASS", "password123"),
        )
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=os.getenv("RABBITMQ_HOST", "localhost"),
                credentials=credentials,
            )
        )
        channel = connection.channel()
        channel.queue_declare(queue='order_queue', durable=True)
        channel.basic_publish(
            exchange='',
            routing_key='order_queue',
            body=json.dumps(order_data),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        connection.close()
        logger.info(f"Đã đẩy order #{order_data.get('order_id')} vào queue")
    except Exception as e:
        logger.error(f"Lỗi publish queue: {e}")

# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"service": "MicroSync Order API", "version": "1.0.0"}


@app.post("/api/v1/orders", status_code=202)
async def create_order(order: OrderCreate, background_tasks: BackgroundTasks):
    """Nhận đơn mới → insert MySQL → đẩy vào RabbitMQ để xử lý async."""
    mysql_conn = None
    try:
        mysql_conn = get_mysql()
        with mysql_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (user_id, product_id, quantity, total_price, status) "
                "VALUES (%s, %s, %s, %s, 'PENDING')",
                (order.user_id, order.product_id, order.quantity, order.total_price),
            )
            order_id = cur.lastrowid
        mysql_conn.commit()

        event = {
            "order_id":   order_id,
            "user_id":    order.user_id,
            "product_id": order.product_id,
            "quantity":   order.quantity,
            "total_price": order.total_price,
        }
        background_tasks.add_task(publish_to_queue, event)

        return {
            "message":  "Order accepted and queued for processing",
            "order_id": order_id,
            "status":   202,
        }

    except Exception as e:
        logger.error(f"Lỗi tạo đơn hàng: {e}")
        if mysql_conn:
            mysql_conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create order: {str(e)}")

    finally:
        if mysql_conn:
            mysql_conn.close()


@app.get("/api/v1/orders")
def list_orders(
    page:   int           = Query(1, ge=1),
    limit:  int           = Query(20, ge=1, le=100),
    status: Optional[str] = Query(None),
):
    """Lấy danh sách đơn hàng từ MySQL (có phân trang & filter theo status)."""
    try:
        mysql_conn = get_mysql()
        with mysql_conn.cursor() as cur:
            if status:
                cur.execute("SELECT COUNT(*) AS cnt FROM orders WHERE status = %s", (status,))
            else:
                cur.execute("SELECT COUNT(*) AS cnt FROM orders")
            total = cur.fetchone()["cnt"]

            offset = (page - 1) * limit
            if status:
                cur.execute(
                    "SELECT o.id, o.user_id, o.product_id, p.name AS product_name, "
                    "o.quantity, o.total_price, o.status, o.created_at "
                    "FROM orders o LEFT JOIN products p ON o.product_id = p.id "
                    "WHERE o.status = %s ORDER BY o.id DESC LIMIT %s OFFSET %s",
                    (status, limit, offset),
                )
            else:
                cur.execute(
                    "SELECT o.id, o.user_id, o.product_id, p.name AS product_name, "
                    "o.quantity, o.total_price, o.status, o.created_at "
                    "FROM orders o LEFT JOIN products p ON o.product_id = p.id "
                    "ORDER BY o.id DESC LIMIT %s OFFSET %s",
                    (limit, offset),
                )
            rows = cur.fetchall()

        mysql_conn.close()

        items = []
        for r in rows:
            r = dict(r)
            if r.get("created_at"):
                r["created_at"] = r["created_at"].isoformat()
            items.append(r)

        return {
            "items":       items,
            "total":       total,
            "page":        page,
            "limit":       limit,
            "total_pages": max(1, (total + limit - 1) // limit),
        }

    except Exception as e:
        logger.error(f"Lỗi lấy danh sách đơn: {e}")
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")


@app.get("/api/v1/orders/{order_id}")
def get_order(order_id: int):
    """Lấy chi tiết 1 đơn hàng."""
    try:
        mysql_conn = get_mysql()
        with mysql_conn.cursor() as cur:
            cur.execute(
                "SELECT o.*, p.name AS product_name FROM orders o "
                "LEFT JOIN products p ON o.product_id = p.id WHERE o.id = %s",
                (order_id,),
            )
            order = cur.fetchone()
        mysql_conn.close()

        if not order:
            raise HTTPException(status_code=404, detail="Order not found")

        order = dict(order)
        if order.get("created_at"):
            order["created_at"] = order["created_at"].isoformat()
        return order

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")