from fastapi import FastAPI, HTTPException
from confluent_kafka import Producer, Consumer, KafkaException
from datetime import datetime
from argparse import ArgumentParser
import logging


def make_fastapi_application(broker_socket) -> FastAPI:
    # create instance of an app
    app = FastAPI()

    # function of callback an error, must be included in CONFIG down below
    def error_cb(err):
        # TODO change to logging system
        print('[ERROR] Callback error:', err)

    config_producer = {'bootstrap.servers': f'{broker_socket}',
                       'error_cb': error_cb,
                       'message.timeout.ms': 10
                       }
    producer = Producer(config_producer)

    # simple get handle
    @app.get("/about")
    async def about():
        return {"Message": "Welcome to FastAPI Stub v 0.01"}

    # handler to send message to kafka
    @app.post("/to-kafka-datetime")
    async def get_one(message: str):

        def delivery_report(err, msg):
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                # TODO change to logging system
                print('[ERROR] Delivery report error: {}'.format(err))
                raise HTTPException(500, f"{err}")
            else:
                # TODO change to logging system
                print('[SUCCESS] Delivery report: {} [{}]'.format(msg.topic(), msg.partition()))

        producer.produce('test1', value=f'<<<{datetime.now()}>>>, {message}', callback=delivery_report)

        # any time flush is called - callback from *produce* (up above)  will be called
        # if a callback will contain some errors - they will be handled inside delivery report
        producer.flush(timeout=1.0)


    # TODO add get from KAFKA handle
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
