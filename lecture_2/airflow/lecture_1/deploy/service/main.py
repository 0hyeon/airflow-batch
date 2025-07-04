import time
import random
import json

import pandas as pd
from confluent_kafka import Producer, Consumer

conf = {"bootstrap.servers": "34.28.227.135:9092", "client.id": "TEST"}
topic = "category-match-in"

producer = Producer(conf)

num = 20

test_data = pd.read_csv("test.csv", header=0)
labels = [
    "과자",
    "디저트",
    "면류",
    "미분류",
    "상온HMR",
    "생활용품",
    "소스",
    "유제품",
    "음료",
    "의약외품",
    "이_미용",
    "주류",
    "커피차",
    "통조림_안주",
    "홈클린",
]

for product in test_data.sample(num).to_dict("records"):
    key = str(random.randint(1, 10))
    value = {"id": key, "input": product["product"]}

    json_msg = json.dumps(value, ensure_ascii=False)

    print(json_msg, labels[int(product["label"])])

    producer.produce(
        topic, key=bytes(key, encoding="utf-8"), value=bytes(json_msg, encoding="utf-8")
    )

    producer.poll(1)

#############################################################################

topic = "category-match-out"
conf = {
    "bootstrap.servers": "34.28.227.135:9092",
    "group.id": "TEST",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": "true",
}

consumer = Consumer(conf)
consumer.subscribe([topic])
while True:
    msg = consumer.poll(timeout=1.0)
    if msg is not None:
        print(json.loads(msg.value()))

consumer.close()
