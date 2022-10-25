import json
import os
import sys

import pika
from configparser import ConfigParser
import os

config = ConfigParser()

config.read("config.ini")
rabbit_mq_host = os.environ.get('RABBIT_MQ_HOST', config.get("datapipeline", "RABBIT_MQ_HOST"))
print ('inside ----')
try:
    from pipeline.model_to_pipeline import * 
    pass
except Exception as e:
    print ('exception ----',e)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_mq_host))
    channel = connection.channel()

    channel.queue_declare(queue='pipeline_ui_queue')

    def callback(ch, method, properties, body):
        body_json = json.loads(body.decode())
        print("Recieved..", body_json)
        p_id = body_json['p_id']
        # print("got p_id as ", body_json['p_id'])
        temp_file_name = body_json['temp_file_name']
        res_details = body_json['res_details']
        db_action = body_json['db_action']
        try:
            task_executor(p_id, temp_file_name, res_details, db_action)
        except Exception as e:
            print (e)
        print(" [x] Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='pipeline_ui_queue', on_message_callback=callback)

    channel.start_consuming()


print("inside demon main...")

try:
    main()
except KeyboardInterrupt:
    print('Interrupted')
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
