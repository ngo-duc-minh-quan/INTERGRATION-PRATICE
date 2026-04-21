# legacy_poller.py (Yêu cầu: pip install pandas)
import pandas as pd
import time
import os

WATCH_DIR = "./legacy_data"
PROCESSED_DIR = "./legacy_data/processed"

def clean_dirty_data(df):
    """Chiến lược xử lý Dữ liệu bẩn"""
    # 1. Bỏ dòng thiếu dữ liệu quan trọng
    df = df.dropna(subset=['Item_Code', 'Qty'])
    
    # 2. Xử lý số lượng (Cast sang Int, loại bỏ số âm)
    df['Qty'] = pd.to_numeric(df['Qty'], errors='coerce')
    df = df[df['Qty'] > 0]
    
    # 3. Chuẩn hóa chuỗi (VD: bỏ dấu $ trong Total_Amt)
    if 'Total_Amt' in df.columns:
        df['Total_Amt'] = df['Total_Amt'].astype(str).str.replace('$', '').str.replace(',', '')
        df['Total_Amt'] = pd.to_numeric(df['Total_Amt'], errors='coerce')
        
    return df

def poll_directory():
    print("Bắt đầu Polling thư mục Legacy...")
    while True:
        for filename in os.listdir(WATCH_DIR):
            if filename.endswith(".csv"):
                filepath = os.path.join(WATCH_DIR, filename)
                print(f"Phát hiện file mới: {filename}")
                
                # Đọc và làm sạch dữ liệu
                raw_data = pd.read_csv(filepath)
                cleaned_data = clean_dirty_data(raw_data)
                
                print(f"Đã làm sạch {len(cleaned_data)} bản ghi hợp lệ. Sẵn sàng insert vào DB.")
                # TODO: Thêm code Insert vào MySQL/Postgres ở đây
                
                # Di chuyển file để tránh đọc lại
                os.rename(filepath, os.path.join(PROCESSED_DIR, filename))
        
        time.sleep(10) # Quét mỗi 10 giây

if __name__ == "__main__":
    if not os.path.exists(PROCESSED_DIR):
        os.makedirs(PROCESSED_DIR)
    poll_directory()