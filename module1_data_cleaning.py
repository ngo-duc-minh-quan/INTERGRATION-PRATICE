import csv
import os
import logging
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Load biến môi trường nếu có
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("Module1_Inventory_Processor")

# Cấu hình Database PostgreSQL (inventory_db)
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASS = os.getenv("PG_PASS", "password123")
PG_DB = os.getenv("PG_DB", "inventory_db")

def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASS,
        dbname=PG_DB
    )

def process_and_import_inventory(csv_file_path):
    """
    Đọc file inventory.csv chứa 5000 dòng.
    Áp dụng chiến lược: MISSING_VALUES
    - Tự động phát hiện lỗi.
    - Dùng try-except để bỏ qua dòng lỗi nghiêm trọng hoặc sửa lỗi nhẹ.
    """
    if not os.path.exists(csv_file_path):
        logger.error(f"Không tìm thấy file: {csv_file_path}")
        return

    valid_records = []
    skipped_count = 0
    fixed_count = 0

    logger.info(f"Bắt đầu đọc file {csv_file_path}...")

    with open(csv_file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row_index, row in enumerate(reader, start=2): # start=2 do dòng 1 là header
            try:
                # 1. Phát hiện MISSING_VALUES
                product_id_str = row.get("product_id", "").strip()
                sku = row.get("sku", "").strip()
                name = row.get("name", "").strip()
                price_str = row.get("price", "").strip()
                stock_qty_str = row.get("stock_qty", "").strip()

                # Kiểm tra lỗi nghiêm trọng: Thiếu product_id hoặc sku thì BỎ QUA (skip)
                if not product_id_str or not sku:
                    raise ValueError(f"MISSING_VALUES: Thiếu product_id hoặc sku tại dòng {row_index}.")

                product_id = int(product_id_str)
                
                # Nếu thiếu name, sửa lỗi (fix) bằng cách đặt tên mặc định
                if not name:
                    name = f"Unknown_Product_{sku}"
                    fixed_count += 1
                
                # Nếu thiếu price, sửa lỗi (fix) bằng cách đặt giá 0 hoặc mặc định
                if not price_str:
                    price = 0.0
                    fixed_count += 1
                else:
                    price = float(price_str)
                
                # Nếu thiếu stock_qty, sửa lỗi (fix) thành 0
                if not stock_qty_str:
                    stock_qty = 0
                    fixed_count += 1
                else:
                    stock_qty = int(stock_qty_str)

                # Dữ liệu đã sạch
                valid_records.append((product_id, sku, name, price, stock_qty))

            except ValueError as ve:
                # Catch lỗi parse số (int/float) hoặc do raise ValueError ở trên
                logger.warning(f"Bỏ qua dòng {row_index}: {ve}")
                skipped_count += 1
            except Exception as e:
                # Catch các lỗi không lường trước khác
                logger.error(f"Lỗi không xác định tại dòng {row_index}: {e}")
                skipped_count += 1

    logger.info(f"Đã xử lý xong file CSV. Hợp lệ: {len(valid_records)}, Bỏ qua: {skipped_count}, Đã sửa: {fixed_count}.")

    # Insert vào PostgreSQL (nếu có dữ liệu hợp lệ)
    if valid_records:
        import_to_db(valid_records)

def import_to_db(records):
    logger.info(f"Bắt đầu import {len(records)} dòng vào PostgreSQL...")
    conn = None
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            # Upsert (Insert, nếu trùng product_id thì Update)
            query = """
                INSERT INTO inventory (product_id, sku, name, price, stock_qty)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE 
                SET sku = EXCLUDED.sku,
                    name = EXCLUDED.name,
                    price = EXCLUDED.price,
                    stock_qty = EXCLUDED.stock_qty,
                    last_updated = CURRENT_TIMESTAMP;
            """
            execute_batch(cur, query, records)
        conn.commit()
        logger.info("Import thành công!")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Lỗi khi import vào DB: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Đường dẫn tới file CSV (bạn hãy đảm bảo file inventory.csv nằm cùng thư mục hoặc sửa đường dẫn này)
    csv_path = os.path.join(os.path.dirname(__file__), "inventory.csv")
    process_and_import_inventory(csv_path)
