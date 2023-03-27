import json
import time

import pandas as pd
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
binding_key = "merge_columns"
result = channel.queue_declare(binding_key, exclusive=False, durable=True)
queue_name = result.method.queue
print(queue_name)

channel.queue_bind(exchange='topic_logs', queue=queue_name, routing_key=binding_key)

def merge_columns(context, data):
    column1, column2, output_column = context['column1'], context['column2'], context['output_column']
    separator = context['separator']
    transformed_data = pd.read_json(data)
    try:
        transformed_data[output_column] = transformed_data[column1].astype(str) + separator + transformed_data[column2].astype(str)
        transformed_data = transformed_data.drop([column1, column2], axis=1)
    except Exception as e:
        return "Worker failed with an error - " + str(e)
    return transformed_data


def on_request(ch, method, props, body):
    print("[x] received task message...")
    task_details = json.loads(body)
    context = task_details["context"]
    data = task_details["data"]
    try:
        response = merge_columns(context, data)
        if isinstance(response, pd.core.frame.DataFrame):
            response_msg = response.to_csv()
        else:
            response_msg = response
        ch.basic_publish(exchange="",
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id=props.correlation_id,delivery_mode=2),
                         body=str(response_msg))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("[x] sent the response to the client..")
    except Exception as e:
        raise e


channel.basic_qos(prefetch_count=2)
channel.basic_consume(queue=queue_name, on_message_callback=on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()