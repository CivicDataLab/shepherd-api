import json

import pandas as pd
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
result = channel.queue_declare('', exclusive=False, durable=True)
queue_name = result.method.queue

print("queue name----", queue_name)
binding_keys = ["skip_column"]

for binding_key in binding_keys:
     channel.queue_bind(
         exchange='topic_logs', queue=queue_name, routing_key=binding_key)

def skip_column(context, data):
    column = context['columns']
    col = column
    data_df = pd.read_json(data)
    if not isinstance(column, list):
        column = list()
        column.append(col)
    try:
        transformed_data = data_df.drop(column, axis=1)
    except:
        raise
    return transformed_data


def on_request(ch, method, props, body):
    task_details = json.loads(body)
    context = task_details["context"]
    data = task_details["data"]
    print("body",context)
    print("hereeee")
    print(type(context))
    response = skip_column(context, data)
    print("response in worker...", response)
    ch.basic_publish(exchange="",
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id,delivery_mode=2),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=2)
channel.basic_consume(queue=queue_name, on_message_callback=on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()