import json
import os
import sys

import django
import pika
# from pipeline.model_to_pipeline import *
import log_utils

print ('inside ----')
try:
    from pipeline.model_to_pipeline import *
    pass
except Exception as e:
    #raise e
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, exc_tb.tb_lineno)
    print ('exception ----',e)
    

# pipeline_object = Pipeline.objects.get(pk=196)
# print(pipeline_object.pipeline_name)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='pipeline_ui_queue')

    def callback(ch, method, properties, body):
        body_json = json.loads(body.decode())
        print("Recieved..", body_json)
        p_id = body_json['p_id']
        logger = log_utils.get_logger_for_existing_file(p_id)
        temp_file_name = body_json['temp_file_name']
        try:
            task_executor(p_id, temp_file_name)
        except Exception as e:
            logger.error(f"""ERROR: Worker demon failed with an error {str(e)}""")
            print (e)
        # print("got temp_file name as ", body_json['temp_file_name'])
        # os.remove('./'+temp_file_name)
        print(" [x] Done")
        logger.info(f"""INFO: Worker demon finished successfully """)
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
