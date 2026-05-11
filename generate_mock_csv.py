import csv
import random
import os

filename = os.path.join(os.path.dirname(__file__), "inventory.csv")
num_rows = 5000

# Khởi tạo mock data với xác suất xuất hiện MISSING_VALUES
with open(filename, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["product_id", "sku", "name", "price", "stock_qty"])
    
    missing_critical = 0
    missing_minor = 0

    for i in range(1, num_rows + 1):
        pid = str(i + 1000)
        sku = f"SKU-{pid}"
        name = f"Mock Product {pid}"
        price = str(random.randint(10, 500) * 1000)
        stock = str(random.randint(0, 500))

        # Giả lập lỗi nghiêm trọng (Skip)
        if random.random() < 0.02: 
            pid = ""  # Thiếu product_id
            missing_critical += 1
        elif random.random() < 0.02: 
            sku = ""  # Thiếu sku
            missing_critical += 1
            
        # Giả lập lỗi nhẹ (Fix)
        if random.random() < 0.05: 
            name = "" # Thiếu name
            missing_minor += 1
        if random.random() < 0.05: 
            price = "" # Thiếu price
            missing_minor += 1
        if random.random() < 0.05: 
            stock = "" # Thiếu stock
            missing_minor += 1
        
        writer.writerow([pid, sku, name, price, stock])

print(f"Đã tạo thành công {num_rows} dòng vào file {filename}")
print(f"Số lỗi nghiêm trọng dự kiến (bị skip): ~{missing_critical}")
print(f"Số lỗi nhẹ dự kiến (được fix): ~{missing_minor}")
