import os
import sys

import pika


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    channel.queue_declare(queue='pipeline_ui_queue')

    def callback(ch, method, properties, body):
        print("Recieved..",body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='pipeline_ui_queue', on_message_callback=callback)

    channel.start_consuming()
try:
    main()
except KeyboardInterrupt:
    print('Interrupted')
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
