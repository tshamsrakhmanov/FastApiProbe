from fastapi import FastAPI, HTTPException
from confluent_kafka import Producer, Consumer
from datetime import datetime
from argparse import ArgumentParser
from time import sleep
import logging
from logging import FileHandler
import sys


def make_fastapi_application(broker_socket, broker_timeout, topic_name) -> FastAPI:
    # create instance of an app
    application = FastAPI()

    # logging configuration

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # handler for Docker-run cases
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    root.addHandler(handler)

    # handler for standalone run cases
    handler2 = FileHandler("stub.log", encoding='utf-8')
    handler2.setLevel(logging.DEBUG)
    handler2.setFormatter(formatter)
    root.addHandler(handler2)

    # function of callback an error, must be included in CONFIG down below
    def error_cb(err):
        logging.error(f'Callback error:{err}')

    # configure KAFKA environment

    config_producer = {'bootstrap.servers': f'{broker_socket}',
                       'error_cb': error_cb,
                       'message.timeout.ms': 1000
                       }

    config_consumer = {'bootstrap.servers': f'{broker_socket}',
                       'group.id': 'group1',
                       'auto.offset.reset': 'latest'}

    # initialize new consumer with each GET request
    consumer = Consumer(config_consumer)
    consumer.subscribe([topic_name])

    # simple get handle
    @application.get("/about")
    async def about():
        return {"Message": "Welcome to FastAPI Stub v 0.9"}

    # handler to send message to kafka
    @application.post("/to-kafka")
    async def to_kafka(message: str):

        def delivery_report(err, msg):
            # Called once for each message produced to indicate delivery result.
            # Triggered by poll() or flush()
            if err is not None:
                logging.error(f'Delivery report error: {err}')
                raise HTTPException(500, f"{err}")
            else:
                logging.info(f'Delivery report: {msg.topic()}, {msg.value()}')

        producer = Producer(config_producer)
        producer.produce(topic_name, value=f'<<<{datetime.now()}>>>, {message}', callback=delivery_report)

        # any time flush is called - callback from *produce* (up above)  will be called
        # if a callback will contain some errors - they will be handled inside delivery report
        producer.flush(timeout=1.0)

    @application.get("/from-kafka")
    async def from_kafka():

        sleep(broker_timeout)
        message_from_kafka = consumer.poll(timeout=0.1)

        # noinspection PyArgumentList
        if message_from_kafka is None:
            logging.info('Kafka internal error: broker down or topic not valid or no new messages in topic')
            raise HTTPException(status_code=500,
                                detail='Kafka internal error: broker down OR topic not valid OR no new messages in topic')
        else:
            transmit_data = message_from_kafka.value()
            logging.info(f'Message fetched and returned:{transmit_data}')
            return transmit_data

    return application


if __name__ == '__main__':
    parser = ArgumentParser()

    parser.add_argument('-s', type=str, required=False, default='localhost:9094',
                        help='Socket of a desired KAFKA. Input as <ip address>:<port>. Default value: "localhost:9094"')
    parser.add_argument('-t', type=float, required=False, default=0.5,
                        help='Time in seconds to wait before message acquisition by GET handler. Default value:"0.5". Input in float.')
    parser.add_argument('-tp', type=str, required=False, default='test1',
                        help='Topic name to which write and read. Default: "test1"')

    args = parser.parse_args()

    app = make_fastapi_application(args.s, args.t, args.tp)

    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=8888)
