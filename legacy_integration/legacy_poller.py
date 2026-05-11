# legacy_integration/legacy_poller.py
# Polls the legacy_data directory for CSV files, cleans "dirty" data,
# and inserts reconciled records into both MySQL (orders) and PostgreSQL (inventory).
# Run: python legacy_poller.py

import os
import time
import logging
import pandas as pd
import pymysql
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

WATCH_DIR = os.path.join(os.path.dirname(__file__), "legacy_data")
PROCESSED_DIR = os.path.join(WATCH_DIR, "processed")
POLL_INTERVAL = 10  # seconds

# ── DB config (reads from env with sensible docker defaults) ──────────────────
MYSQL_CFG = dict(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", 3307)),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", "root"),
    database=os.getenv("MYSQL_DB", "order_db"),
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

PG_CFG = dict(
    host=os.getenv("PG_HOST", "127.0.0.1"),
    port=int(os.getenv("PG_PORT", 5432)),
    user=os.getenv("PG_USER", "admin"),
    password=os.getenv("PG_PASSWORD", "password123"),
    dbname=os.getenv("PG_DB", "inventory_db"),
)


# ── Data-cleaning logic ────────────────────────────────────────────────────────
def clean_dirty_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reconciliation rules for dirty legacy CSV files:
      1. Drop rows missing Item_Code or Qty
      2. Cast Qty to numeric; drop negatives / zero
      3. Strip currency symbols from Total_Amt and cast to float
      4. Normalise Item_Code (strip whitespace, uppercase)
    """
    initial = len(df)

    # 1. Required columns
    df = df.dropna(subset=["Item_Code", "Qty"])

    # 2. Qty must be positive integer
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce")
    df = df[df["Qty"] > 0].copy()
    df["Qty"] = df["Qty"].astype(int)

    # 3. Monetary field
    if "Total_Amt" in df.columns:
        df["Total_Amt"] = (
            df["Total_Amt"]
            .astype(str)
            .str.replace(r"[$,\s]", "", regex=True)
        )
        df["Total_Amt"] = pd.to_numeric(df["Total_Amt"], errors="coerce")

    # 4. Normalise SKU
    df["Item_Code"] = df["Item_Code"].astype(str).str.strip().str.upper()

    dropped = initial - len(df)
    if dropped:
        log.warning("  ⚠  Dropped %d dirty rows (missing/invalid data)", dropped)

    log.info("  ✔  %d clean records ready for reconciliation", len(df))
    return df.reset_index(drop=True)


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_mysql():
    return pymysql.connect(**MYSQL_CFG)


def get_pg():
    return psycopg2.connect(**PG_CFG)


def reconcile_and_insert(df: pd.DataFrame):
    """
    For each clean row:
      • Check PostgreSQL inventory stock for the SKU.
      • If stock >= requested Qty  → insert order into MySQL as 'completed'.
      • If stock < requested Qty   → insert order as 'out_of_stock'; log mismatch.
      • If SKU not found           → insert order as 'error'; log unknown SKU.
      After a successful order, deduct stock from PostgreSQL.
    """
    if df.empty:
        return

    mysql_conn = get_mysql()
    pg_conn = get_pg()
    matched = mismatch = out_of_stock = errors = 0

    try:
        with mysql_conn.cursor() as mysql_cur, pg_conn.cursor() as pg_cur:
            for _, row in df.iterrows():
                sku = row["Item_Code"]
                qty = int(row["Qty"])
                total_amt = float(row.get("Total_Amt", 0) or 0)

                # ── query current PostgreSQL stock ────────────────────────────
                pg_cur.execute(
                    "SELECT id, stock_quantity FROM inventory WHERE sku = %s FOR UPDATE",
                    (sku,),
                )
                inv = pg_cur.fetchone()

                if inv is None:
                    status = "error"
                    errors += 1
                    log.error("  ✖  SKU '%s' not found in inventory", sku)
                elif inv[1] < qty:
                    status = "out_of_stock"
                    out_of_stock += 1
                    log.warning(
                        "  ⚠  SKU '%s' stock=%d < requested=%d", sku, inv[1], qty
                    )
                else:
                    status = "completed"
                    # Deduct stock in PostgreSQL
                    pg_cur.execute(
                        "UPDATE inventory SET stock_quantity = stock_quantity - %s, "
                        "updated_at = NOW() WHERE id = %s",
                        (qty, inv[0]),
                    )
                    matched += 1

                # ── insert order record into MySQL ────────────────────────────
                mysql_cur.execute(
                    """
                    INSERT INTO orders
                        (customer_id, product_id, quantity, total_amount, status, created_at)
                    VALUES
                        (1, %s, %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE status = VALUES(status)
                    """,
                    (sku[:50], qty, total_amt, status),
                )

        mysql_conn.commit()
        pg_conn.commit()

    except Exception as exc:
        mysql_conn.rollback()
        pg_conn.rollback()
        log.exception("Transaction rolled back: %s", exc)
    finally:
        mysql_conn.close()
        pg_conn.close()

    log.info(
        "  📊 Results — matched: %d | mismatch/out-of-stock: %d | unknown SKU: %d",
        matched,
        out_of_stock,
        errors,
    )


# ── Main polling loop ─────────────────────────────────────────────────────────
def poll_directory():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    log.info("🔍 Legacy poller started — watching: %s", WATCH_DIR)

    while True:
        csv_files = [f for f in os.listdir(WATCH_DIR) if f.endswith(".csv")]

        if not csv_files:
            log.debug("No new files. Sleeping %ds…", POLL_INTERVAL)
        else:
            for filename in csv_files:
                filepath = os.path.join(WATCH_DIR, filename)
                log.info("📄 Processing: %s", filename)

                try:
                    raw = pd.read_csv(filepath)
                    cleaned = clean_dirty_data(raw)
                    reconcile_and_insert(cleaned)
                    # Move to processed so it won't be read again
                    dest = os.path.join(PROCESSED_DIR, filename)
                    os.rename(filepath, dest)
                    log.info("  ↪  Moved to processed/: %s", filename)
                except Exception as exc:
                    log.exception("Failed to process %s: %s", filename, exc)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    poll_directory()