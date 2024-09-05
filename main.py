from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer, Consumer, KafkaException
from datetime import datetime

# config data can be described in separate file
config = {
    'bootstrap.servers': 'localhost:9090',
    'group.id': 'group1',
    'auto.offset.reset': 'earliest',
}

#
# class Item(BaseModel):
#     name: str
#     price: float
#     brand: str = None


producer = Producer(config)
consumer = Consumer(config)
consumer.subscribe(['test_topic_name'])

app = FastAPI()


def send_message(topic, message):
    producer.produce(topic, value=message)
    producer.flush()


@app.get("/get-1")
async def get_one():
    send_message('test_topic_name', f'[INTO]{datetime.now()}')
    return {"Status": "Success"}


@app.get("/get-2")
async def get_one():
    temp_data = {}
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # ожидание сообщения
            if msg is None:  # если сообщений нет
                continue
            if msg.error():  # обработка ошибок
                raise KafkaException(msg.error())
            else:
                # действия с полученным сообщением
                print(f"Received message: {msg.value().decode('utf-8')}")
                temp_data.setdefault('message', f'{msg.value().decode('utf-8')}')
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()  # не забываем закрыть соединение
    return temp_data

#
# template_db = {1: {"name": "first", "type": "first name"}, 2: {"name": "second", "type": "second name"}}
#
#
# @app.get("/")
# async def root():
#     return {"message": "Hello World"}
#
#
# @app.get("/about")
# async def about():
#     return {"message": "about"}
#
#
# @app.get("/get-item/{item_id}")
# async def get_item(item_id: int):
#     return template_db[item_id]
#
#
# @app.get("/get-by-name/{item_id}")
# async def get_item(item_id: int, name: str = None):
#     for item_id in template_db:
#         if template_db[item_id]["name"] == name:
#             return template_db[item_id]
#     return {"Data": "Not found"}
#
#
# @app.post("/create-item")
# def create_item(item: Item):
#     return {}
