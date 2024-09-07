from fastapi import FastAPI, HTTPException
from confluent_kafka import Producer, Consumer, KafkaException
from datetime import datetime
from argparse import ArgumentParser


def make_fastapi_application(broker_socket):
    kafka_broker_socket: str = broker_socket

    # create instance of an app
    app = FastAPI()

    # simple get handle
    @app.get("/about")
    async def about():
        return {"Message": "Welcome to FastAPI Stub v 0.01"}

    # handler to send message to kafka
    @app.post("/to-kafka-datetime")
    async def get_one(message: str):
        config = {'bootstrap.servers': f'{kafka_broker_socket}'}
        producer = Producer(config)

        producer.produce('test1', value=f'[INTO]{datetime.now()}, {message}')
        producer.flush()

    # return app with all handlers to run it in uvicorn server
    return app


if __name__ == '__main__':
    parser = ArgumentParser()

    parser.add_argument('--broker-socket', type=str, required=True,
                        help='NAME:PORT of reired kafka in cluster. Name - are ')
    args = parser.parse_args()

    app = make_fastapi_application(args.broker_socket)

    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=8888)
