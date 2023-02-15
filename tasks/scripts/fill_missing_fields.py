import json

import numpy as np
import pandas as pd
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
result = channel.queue_declare('', exclusive=False, durable=True)
queue_name = result.method.queue

print("queue name----", queue_name)
binding_key = "fill_missing_fields"

channel.queue_bind(exchange='topic_logs', queue=queue_name, routing_key=binding_key)


def fill_missing_fields(context, data):
    column = context['column']
    data = pd.read_json(data)
    try:
        transformed_data = data.fillna({column: np.NAN})
    except Exception as e:
        return "Worker failed with an error - " + str(e)
    return transformed_data


def on_request(ch, method, props, body):
    print("[x] received task message...")
    task_details = json.loads(body)
    context = task_details["context"]
    data = task_details["data"]
    try:
        response = fill_missing_fields(context, data)
        if isinstance(response, pd.core.frame.DataFrame):
            response_msg = response.to_csv()
        else:
            response_msg = response
            print(response_msg)
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
