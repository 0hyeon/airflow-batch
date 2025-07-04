from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="34.28.227.135:9093",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

products = [
    "에어팟",
    "갤럭시 버즈",
    "아이폰 15",
    "갤럭시 S24",
    "아이패드",
    "맥북",
    "LG 그램",
]
statuses = ["배송완료", "배송중", "결제완료", "상품준비중", "취소됨"]
categories = ["전자기기", "스마트폰", "태블릿", "노트북", "액세서리"]

start_time = time.time()
messages_sent = 0
duration = 60  # 60초 동안


def generate_message(i):
    product = random.choice(products)
    original_price = random.randint(50000, 1500000)
    discount_rate = random.choice([0, 5, 10, 15, 20])
    discount_price = int(original_price * (1 - discount_rate / 100))
    return {
        "id": f"order_{i}",
        "status": random.choice(statuses),
        "product": product,
        "price": original_price,
        "discount_price": discount_price,
        "discount_rate": discount_rate,
        "category": random.choice(categories),
        "created_at": datetime.utcnow(),
    }


tps = 5000
total_messages = tps * duration

print(f"🔥 Sending {total_messages} messages to Kafka at {tps} TPS")

for i in range(total_messages):
    producer.send("category-match-in", generate_message(i))
    messages_sent += 1

    # 1초마다 5000건 보내기 위해 슬립
    if messages_sent % tps == 0:
        time.sleep(1)

elapsed = time.time() - start_time
print(f"✅ Done. Sent {messages_sent} messages in {elapsed:.2f} seconds")
producer.close()
