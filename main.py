from fastapi import FastAPI, HTTPException
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from datetime import datetime
from argparse import ArgumentParser
from time import sleep


def make_fastapi_application(broker_socket, broker_timeout) -> FastAPI:
    # create instance of an app
    application = FastAPI()

    # function of callback an error, must be included in CONFIG down below
    def error_cb(err):
        # TODO change to logging system
        print('[STUB Logger] [ERROR] Callback error:', err)

    config_producer = {'bootstrap.servers': f'{broker_socket}',
                       'error_cb': error_cb,
                       'message.timeout.ms': 10
                       }

    config_consumer = {'bootstrap.servers': f'{broker_socket}',
                       'group.id': 'foo',
                       'auto.offset.reset': 'smallest'}

    producer = Producer(config_producer)

    consumer = Consumer(config_consumer)

    consumer.subscribe(["test1"])

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
                # TODO change to logging system
                print('[STUB Logger] [ERROR] Delivery report error: {}'.format(err))
                raise HTTPException(500, f"{err}")
            else:
                # TODO change to logging system
                print('[STUB Logger] [INFO ] Delivery report: {} [{}]'.format(msg.topic(), msg.partition()))

        producer.produce('test1', value=f'<<<{datetime.now()}>>>, {message}', callback=delivery_report)

        # any time flush is called - callback from *produce* (up above)  will be called
        # if a callback will contain some errors - they will be handled inside delivery report
        producer.flush(timeout=1.0)

    @application.get("/from-kafka")
    async def from_kafka():

        sleep(float(broker_timeout))
        msg = consumer.poll(timeout=0.1)

        if msg is None:
            # TODO change to logging system
            print('[STUB Logger] [INFO ] No new messages in Kafka')
            raise HTTPException(status_code=404, detail="No new messages")
        if msg.error():
            # TODO change to logging system
            print('[STUB Logger] [ERROR] Error on: {}'.format(msg.error().str()))
            raise HTTPException(status_code=404, detail="Error occurred")

        # TODO change to logging system
        print('[STUB Logger] [INFO ] Retrieved message from kafaka: {}'.format(msg.value().decode('utf-8')))
        return {"message": msg.value().decode('utf-8')}

    return application


if __name__ == '__main__':
    parser = ArgumentParser()

    parser.add_argument('--broker-socket', type=str, required=True,
                        help='NAME:PORT of reired kafka in cluster. Name - are ')
    parser.add_argument('--broker-timeout', type=str, required=True,
                        help='time in seconds to wait before message acquisition')

    args = parser.parse_args()

    app = make_fastapi_application(args.broker_socket, args.broker_timeout)

    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=8888)
