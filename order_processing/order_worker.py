# order_worker.py
import pika
import json
import time

def process_order(ch, method, properties, body):
    order = json.loads(body)
    print(f"[x] Nhận đơn hàng: {order}")
    
    try:
        # TODO: Kết nối Postgres để trừ tồn kho
        print("  -> Đang kiểm tra tồn kho (Postgres)...")
        time.sleep(1) # Giả lập I/O delay
        
        # TODO: Kết nối MySQL để lưu đơn
        print("  -> Đang lưu đơn hàng (MySQL)...")
        
        print("[v] Xử lý thành công. Gửi ACK.")
        ch.basic_ack(delivery_tag=method.delivery_tag) # Quan trọng: Xác nhận đã xử lý
        
    except Exception as e:
        print(f"[!] Lỗi xử lý: {e}. Từ chối Message (NACK).")
        # Đẩy lại vào queue hoặc Dead Letter Queue
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_worker():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', credentials=pika.PlainCredentials('admin', 'password123'))
    )
    channel = connection.channel()
    channel.queue_declare(queue='order_queue', durable=True)
    
    # Chỉ lấy 1 tin nhắn mỗi lần (Fair dispatch)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='order_queue', on_message_callback=process_order)
    
    print(' [*] Worker đang chờ Message. Nhấn CTRL+C để thoát.')
    channel.start_consuming()

if __name__ == '__main__':
    start_worker()