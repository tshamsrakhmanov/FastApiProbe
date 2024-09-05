from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer, Consumer, KafkaException
from datetime import datetime
from pydantic import BaseModel
from argparse import ArgumentParser
from typing import Final


class Message(BaseModel):
    message: str


def application(broker_socket):
    BROKER_SOKET: Final[str] = broker_socket

    app = FastAPI()

    @app.get("/about")
    async def about():
        return {"Message": "Welcome to FastAPI Stub v 0.01"}

    @app.post("/to-kafka-datetime")
    async def get_one(message: Message):

        recieved_message = message

        config = {'bootstrap.servers': f'{BROKER_SOKET}'}
        producer = Producer(config)

        def send_message(topic, message):
            producer.produce(topic, value=message)
            producer.flush()

        send_message('test1', f'[INTO]{datetime.now()}, {recieved_message}')

        return {"Status": "Success"}

    @app.get("/from-kafka")
    async def get_one():

        try:
            config = {
                'bootstrap.servers': f'{BROKER_SOKET}',
                'group.id': 'mygroup1',
                'auto.offset.reset': 'earliest'
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
        except Exception as e:
            return {"Status": f"{e.__class__.__name__}"}

    return app


if __name__ == '__main__':
    parser = ArgumentParser()

    parser.add_argument('--broker-socket', type=str, required=True,
                        help='NAME:PORT of reired kafka in cluster. Name - are ')
    args = parser.parse_args()

    # print(args.kafka_broker, args.kafka_topic, args.timeout)
    app = application(args.broker_socket)

    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=8888)
