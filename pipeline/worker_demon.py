import json
import os
import sys

import pika
from pipeline.model_to_pipeline import *

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='pipeline_ui_queue')

    def callback(ch, method, properties, body):
        body_json = json.loads(body.decode())
        print("Recieved..", body_json)
        p_id = body_json['p_id']
        # print("got p_id as ", body_json['p_id'])
        temp_file_name = body_json['temp_file_name']
        task_executor(p_id, temp_file_name)
        # print("got temp_file name as ", body_json['temp_file_name'])
        # os.remove('./'+temp_file_name)
        print(" [x] Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='pipeline_ui_queue', on_message_callback=callback)

    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)