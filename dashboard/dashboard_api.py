# dashboard/dashboard_api.py
# FastAPI service phục vụ dữ liệu thật cho dashboard
# Chạy: uvicorn dashboard_api:app --port 8001 --reload

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

import pymysql
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="MicroSync Dashboard API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── DB Connection helpers ───────────────────────────────────────────────────

def get_mysql():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3307)),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASS", "root"),
        database=os.getenv("MYSQL_DB", "order_db"),
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
    )

def get_postgres():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        user=os.getenv("PG_USER", "admin"),
        password=os.getenv("PG_PASS", "password123"),
        dbname=os.getenv("PG_DB", "inventory_db"),
        connect_timeout=5,
    )

# ─── Reconciliation Logic ────────────────────────────────────────────────────

def classify_order(order: dict, inventory: dict) -> str:
    """Phân loại trạng thái đối soát của một đơn hàng."""
    status = order.get("status", "PENDING")
    if status == "COMPLETED":
        return "MATCHED"
    if status.startswith("FAILED"):
        return "FAILED"
    if inventory is None:
        return "PENDING"
    stock = inventory.get("stock_qty", 0)
    qty   = order.get("quantity", 0)
    if stock == 0:
        return "OUT_OF_STOCK"
    if qty > stock:
        return "QTY_MISMATCH"
    return "PENDING"

# ─── Endpoints ───────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"service": "MicroSync Dashboard API", "version": "1.0.0", "status": "running"}


@app.get("/api/stats")
def get_stats():
    """KPI cards: tổng đơn, matched, errors, pending, success_rate."""
    try:
        mysql_conn = get_mysql()
        pg_conn    = get_postgres()

        with mysql_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM orders")
            total = cur.fetchone()["total"]

            cur.execute("SELECT COUNT(*) AS cnt FROM orders WHERE status = 'COMPLETED'")
            completed = cur.fetchone()["cnt"]

            cur.execute("SELECT COUNT(*) AS cnt FROM orders WHERE status LIKE 'FAILED%'")
            failed = cur.fetchone()["cnt"]

            cur.execute("SELECT COUNT(*) AS cnt FROM orders WHERE status = 'PENDING'")
            pending = cur.fetchone()["cnt"]

        with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM inventory WHERE stock_qty = 0")
            oos_products = cur.fetchone()["cnt"]

            cur.execute("SELECT COUNT(*) AS cnt FROM inventory")
            total_inventory = cur.fetchone()["cnt"]

        mysql_conn.close()
        pg_conn.close()

        errors = failed
        success_rate = round((completed / total * 100), 1) if total > 0 else 0.0

        return {
            "total": total,
            "matched": completed,
            "pending": pending,
            "errors": errors,
            "success_rate": success_rate,
            "oos_products": int(oos_products),
            "total_inventory_products": int(total_inventory),
        }

    except Exception as e:
        logger.error(f"Error in /api/stats: {e}")
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")


@app.get("/api/reconciliation")
def get_reconciliation(
    page: int   = Query(1, ge=1),
    limit: int  = Query(20, ge=1, le=100),
    status: str = Query("ALL"),
    search: str = Query(""),
):
    """Bảng đối soát có phân trang & filter theo trạng thái."""
    try:
        mysql_conn = get_mysql()
        pg_conn    = get_postgres()

        # Lấy tất cả inventory vào dict để JOIN nhanh
        with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT product_id, sku, name, stock_qty FROM inventory")
            inventory_map = {row["product_id"]: dict(row) for row in cur.fetchall()}

        offset = (page - 1) * limit

        # Xây dựng query MySQL
        where_clauses = []
        params        = []

        if search:
            where_clauses.append("o.id LIKE %s")
            params.append(f"%{search}%")

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        with mysql_conn.cursor() as cur:
            # Lấy tất cả orders (cần để tính recon_status trước khi filter)
            cur.execute(
                f"SELECT o.id, o.product_id, o.quantity, o.total_price, o.status, "
                f"o.created_at, p.name AS product_name "
                f"FROM orders o LEFT JOIN products p ON o.product_id = p.id "
                f"{where_sql} ORDER BY o.id DESC",
                params,
            )
            all_orders = cur.fetchall()

        mysql_conn.close()
        pg_conn.close()

        # Classify và filter
        results = []
        for order in all_orders:
            inv   = inventory_map.get(order["product_id"])
            recon = classify_order(order, inv)
            if status != "ALL" and recon != status:
                continue
            results.append({
                "id":           order["id"],
                "product_id":   order["product_id"],
                "product_name": order["product_name"] or f"Product_{order['product_id']}",
                "sku":          inv["sku"] if inv else f"SKU-{order['product_id']}",
                "quantity":     order["quantity"],
                "total_price":  float(order["total_price"]),
                "db_status":    order["status"],
                "recon_status": recon,
                "stock_qty":    inv["stock_qty"] if inv else None,
                "created_at":   order["created_at"].isoformat() if order["created_at"] else None,
            })

        total_filtered = len(results)
        paginated      = results[offset: offset + limit]

        return {
            "items":       paginated,
            "total":       total_filtered,
            "page":        page,
            "limit":       limit,
            "total_pages": max(1, (total_filtered + limit - 1) // limit),
        }

    except Exception as e:
        logger.error(f"Error in /api/reconciliation: {e}")
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")


@app.get("/api/reconciliation/{order_id}")
def get_order_detail(order_id: int):
    """Chi tiết 1 đơn hàng kèm thông tin tồn kho."""
    try:
        mysql_conn = get_mysql()
        pg_conn    = get_postgres()

        with mysql_conn.cursor() as cur:
            cur.execute(
                "SELECT o.*, p.name AS product_name, p.price AS unit_price "
                "FROM orders o LEFT JOIN products p ON o.product_id = p.id "
                "WHERE o.id = %s",
                (order_id,),
            )
            order = cur.fetchone()

        if not order:
            raise HTTPException(status_code=404, detail="Order not found")

        with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM inventory WHERE product_id = %s",
                (order["product_id"],),
            )
            inv = cur.fetchone()

        mysql_conn.close()
        pg_conn.close()

        recon = classify_order(order, dict(inv) if inv else None)
        return {
            "order":        {**order, "created_at": order["created_at"].isoformat() if order.get("created_at") else None},
            "inventory":    dict(inv) if inv else None,
            "recon_status": recon,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in /api/reconciliation/{order_id}: {e}")
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")


@app.get("/api/orders/timeline")
def get_orders_timeline():
    """Dữ liệu line chart: số đơn theo ngày."""
    try:
        mysql_conn = get_mysql()
        with mysql_conn.cursor() as cur:
            cur.execute(
                "SELECT DATE(created_at) AS day, COUNT(*) AS count "
                "FROM orders "
                "GROUP BY DATE(created_at) "
                "ORDER BY day ASC "
                "LIMIT 60"
            )
            rows = cur.fetchall()
        mysql_conn.close()

        return {
            "labels": [str(r["day"]) for r in rows],
            "values": [r["count"] for r in rows],
        }

    except Exception as e:
        logger.error(f"Error in /api/orders/timeline: {e}")
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")


@app.get("/api/products/top-mismatch")
def get_top_mismatch(limit: int = Query(10, ge=1, le=50)):
    """Top products có nhiều đơn nhất (tất cả đều PENDING = tiềm năng lỗi)."""
    try:
        mysql_conn = get_mysql()
        pg_conn    = get_postgres()

        with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT product_id, sku, name, stock_qty FROM inventory WHERE stock_qty = 0")
            oos_ids = {row["product_id"] for row in cur.fetchall()}

        with mysql_conn.cursor() as cur:
            cur.execute(
                "SELECT product_id, p.name AS product_name, COUNT(*) AS order_count, "
                "SUM(quantity) AS total_qty "
                "FROM orders o LEFT JOIN products p ON o.product_id = p.id "
                "WHERE o.status = 'PENDING' "
                "GROUP BY product_id, p.name "
                "ORDER BY order_count DESC "
                "LIMIT %s",
                (limit,),
            )
            rows = cur.fetchall()

        mysql_conn.close()
        pg_conn.close()

        return [
            {
                "product_id":   r["product_id"],
                "product_name": r["product_name"] or f"Product_{r['product_id']}",
                "order_count":  r["order_count"],
                "total_qty":    r["total_qty"],
                "is_oos":       r["product_id"] in oos_ids,
            }
            for r in rows
        ]

    except Exception as e:
        logger.error(f"Error in /api/products/top-mismatch: {e}")
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")


@app.post("/api/reconciliation/{order_id}/resolve")
def resolve_order(order_id: int):
    """Đánh dấu đơn hàng đã xử lý thủ công (COMPLETED)."""
    try:
        mysql_conn = get_mysql()
        with mysql_conn.cursor() as cur:
            cur.execute(
                "UPDATE orders SET status = 'COMPLETED' WHERE id = %s AND status = 'PENDING'",
                (order_id,),
            )
            affected = cur.rowcount
        mysql_conn.commit()
        mysql_conn.close()

        if affected == 0:
            raise HTTPException(status_code=404, detail="Order not found or already resolved")

        return {"message": f"Order {order_id} resolved successfully", "order_id": order_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in /api/reconciliation/{order_id}/resolve: {e}")
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")
