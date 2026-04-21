# order_api.py (Yêu cầu: pip install fastapi uvicorn pika)
from fastapi import FastAPI, BackgroundTasks
import pika
import json

app = FastAPI()
@app.get("/")
def read_root():
    return {"message": "Welcome to Order Microservice API"}
def publish_to_queue(order_data):
    """Gửi Event vào RabbitMQ"""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', credentials=pika.PlainCredentials('admin', 'password123'))
    )
    channel = connection.channel()
    channel.queue_declare(queue='order_queue', durable=True)
    
    channel.basic_publish(
        exchange='',
        routing_key='order_queue',
        body=json.dumps(order_data),
        properties=pika.BasicProperties(delivery_mode=2) # Persistent message
    )
    connection.close()

@app.post("/api/v1/orders", status_code=202)
async def create_order(order: dict, background_tasks: BackgroundTasks):
    """API trả về 202 ngay lập tức, đẩy việc xử lý vào Background/Queue"""
    # Bắn vào Queue
    background_tasks.add_task(publish_to_queue, order)
    return {"message": "Order Accepted and Processing", "status": 202}

# Chạy server: uvicorn order_api:app --reload