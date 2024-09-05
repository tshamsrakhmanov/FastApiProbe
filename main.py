from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer, Consumer, KafkaException
from datetime import datetime

#
# class Item(BaseModel):
#     name: str
#     price: float
#     brand: str = None


app = FastAPI()


@app.get("/about")
async def about():
    return {"Message": "Welcome to FastAPI Stub v 0.01"}


@app.get("/to-kafka-datetime")
async def get_one():
    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    }

    producer = Producer(config)

    def send_message(topic, message):
        producer.produce(topic, value=message)
        producer.flush()

    send_message('test1', f'[INTO]{datetime.now()}')
    return {"Status": "Success"}


@app.get("/from-kafka")
async def get_one():
    config = {
        'bootstrap.servers': 'localhost:9092',  # Список серверов Kafka
        'group.id': 'mygroup',  # Идентификатор группы потребителей
        'auto.offset.reset': 'earliest'  # Начальная точка чтения ('earliest' или 'latest')
    }

    consumer = Consumer(config)
    consumer.subscribe(['test1'])

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
