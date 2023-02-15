import pandas as pd
from sqlalchemy.future import create_engine
import sqlite3
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
binding_key = "db_loader"

channel.queue_bind(exchange='topic_logs', queue=queue_name, routing_key=binding_key)


def get_connection_object(
        dbms_name: str,
        port: str,
        host: str,
        user_name: str,
        password: str,
        db_name: str):
    if dbms_name == 'postgresql':
        return create_engine(f'{dbms_name}://{user_name}:{password}@{host}:{port}/{db_name}')
    elif dbms_name == 'sqlite':
        return sqlite3.connect(db_name)


def populate_db(context, data):
    dbms_name = context['dbms_name']
    port = context['port']
    host = context['host']
    user_name = context['user_name']
    password = context['password']
    db_name = context["db_name"]
    table_name = context["table_name"]
    data = pd.read_json(data)
    engine = get_connection_object(dbms_name=dbms_name, host=host, user_name=user_name,
                               password=password, db_name=db_name, port=port)
    try:
      data.to_sql(table_name, engine, if_exists='append', index=False)
    except Exception as e:
        return "Worker failed with an error - " + str(e)
    return "Successful"


def on_request(ch, method, props, body):
    print("[x] received task message...")
    task_details = json.loads(body)
    context = task_details["context"]
    data = task_details["data"]
    try:
        response = populate_db(context, data)
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
