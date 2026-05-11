# order_processing/order_worker.py
# Worker lắng nghe RabbitMQ, xử lý đơn hàng thật với MySQL + PostgreSQL
# Chạy: python order_worker.py

import os
import json
import time
import logging

import pika
import pymysql
import psycopg2
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

# ─── DB helpers ──────────────────────────────────────────────────────────────

def get_mysql():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3307)),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASS", "root"),
        database=os.getenv("MYSQL_DB", "order_db"),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

def get_postgres():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        user=os.getenv("PG_USER", "admin"),
        password=os.getenv("PG_PASS", "password123"),
        dbname=os.getenv("PG_DB", "inventory_db"),
    )

# ─── Core processing logic ────────────────────────────────────────────────────

def process_order(ch, method, properties, body):
    order = json.loads(body)
    order_id   = order.get("order_id")
    product_id = order.get("product_id")
    quantity   = order.get("quantity", 0)

    logger.info(f"[>] Nhận đơn hàng #{order_id} — product={product_id}, qty={quantity}")

    mysql_conn = None
    pg_conn    = None

    try:
        # ── Step 1: Kết nối PostgreSQL → kiểm tra tồn kho ──────────────────
        pg_conn = get_postgres()
        pg_conn.autocommit = False
        with pg_conn.cursor() as cur:
            # Lock row để tránh race condition
            cur.execute(
                "SELECT stock_qty FROM inventory WHERE product_id = %s FOR UPDATE",
                (product_id,)
            )
            row = cur.fetchone()

        if row is None:
            logger.warning(f"  [!] Product {product_id} không tồn tại trong inventory")
            new_status = "FAILED_NO_PRODUCT"
        elif row[0] < quantity:
            logger.warning(f"  [!] Không đủ tồn kho: có {row[0]}, cần {quantity}")
            new_status = "FAILED_OOS"
        else:
            # ── Step 2: Trừ tồn kho trong PostgreSQL ────────────────────────
            with pg_conn.cursor() as cur:
                cur.execute(
                    "UPDATE inventory SET stock_qty = stock_qty - %s, "
                    "last_updated = NOW() WHERE product_id = %s",
                    (quantity, product_id)
                )
            pg_conn.commit()
            logger.info(f"  [✓] Đã trừ {quantity} tồn kho của product {product_id}")
            new_status = "COMPLETED"

        # ── Step 3: Cập nhật trạng thái đơn hàng trong MySQL ────────────────
        mysql_conn = get_mysql()
        with mysql_conn.cursor() as cur:
            cur.execute(
                "UPDATE orders SET status = %s WHERE id = %s",
                (new_status, order_id)
            )
        mysql_conn.commit()
        logger.info(f"  [✓] Order #{order_id} → status = {new_status}")

        # ACK thành công
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"[✓] Hoàn tất xử lý đơn hàng #{order_id}")

    except Exception as e:
        logger.error(f"[!] Lỗi xử lý đơn #{order_id}: {e}")
        # Rollback nếu cần
        try:
            if pg_conn:
                pg_conn.rollback()
            if mysql_conn:
                mysql_conn.rollback()
        except Exception:
            pass
        # NACK — không requeue để tránh vòng lặp lỗi vô hạn
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    finally:
        if pg_conn:
            try: pg_conn.close()
            except Exception: pass
        if mysql_conn:
            try: mysql_conn.close()
            except Exception: pass


# ─── Worker startup ───────────────────────────────────────────────────────────

def start_worker(max_retries: int = 5):
    retry = 0
    while retry < max_retries:
        try:
            credentials = pika.PlainCredentials(
                os.getenv("RABBITMQ_USER", "admin"),
                os.getenv("RABBITMQ_PASS", "password123"),
            )
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=os.getenv("RABBITMQ_HOST", "localhost"),
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )
            )
            channel = connection.channel()
            channel.queue_declare(queue='order_queue', durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='order_queue', on_message_callback=process_order)

            logger.info(' [*] Worker đang chờ message. Nhấn CTRL+C để thoát.')
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            retry += 1
            wait = 2 ** retry
            logger.warning(f"[!] Mất kết nối RabbitMQ ({e}). Thử lại sau {wait}s... ({retry}/{max_retries})")
            time.sleep(wait)
        except KeyboardInterrupt:
            logger.info("Worker dừng theo yêu cầu.")
            break

    logger.error("Không thể kết nối RabbitMQ sau nhiều lần thử.")


if __name__ == '__main__':
    start_worker()