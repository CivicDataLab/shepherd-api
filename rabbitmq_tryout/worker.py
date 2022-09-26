import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
result = channel.queue_declare('', exclusive=True)
queue_name = result.method.queue

print("queue name----", queue_name)
binding_keys = ["merge_column", "skip_column"]

for binding_key in binding_keys:
     channel.queue_bind(
         exchange='logs_topic', queue=queue_name, routing_key=binding_key)

def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


def on_request(ch, method, props, body):
    print(body)
    n = 0
    print(" [.] fib(%s)" % n)
    response = fib(n)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue_name, on_message_callback=on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()













# connection = pika.BlockingConnection(
#     pika.ConnectionParameters(host='localhost'))
# channel = connection.channel()
#
# channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
#
# result = channel.queue_declare('', exclusive=True)
# queue_name = result.method.queue
# print(queue_name)
# binding_keys = ["merge_column", "skip_column"]
#
# for binding_key in binding_keys:
#     channel.queue_bind(
#         exchange='logs_topic', queue=queue_name, routing_key=binding_key)
#
# print(' [*] Waiting for logs. To exit press CTRL+C')
#
#
# def callback(ch, method, properties, body):
#     print(" [x] %r:%r" % (method.routing_key, body))
#
#
# channel.basic_consume(
#     queue=queue_name, on_message_callback=callback, auto_ack=True)
#
# channel.start_consuming()
