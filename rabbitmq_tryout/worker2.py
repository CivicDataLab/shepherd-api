import pika
import change_format

def execute_engine(binding_keys):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))

    channel = connection.channel()
    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
    result = channel.queue_declare('', exclusive=False, durable=True)
    queue_name = result.method.queue

    print("queue name----", queue_name)

    for binding_key in binding_keys:
         channel.queue_bind(
             exchange='topic_logs', queue=queue_name, routing_key=binding_key)

    def on_request(ch, method, props, body):
        n = 0
        print(" [.] fib(%s)" % n)
        response = change_format.change_format("json")
        ch.basic_publish(exchange="",
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id=props.correlation_id,delivery_mode=2),
                         body=str(response))
        ch.basic_ack(delivery_tag=method.delivery_tag)


    channel.basic_qos(prefetch_count=2)
    channel.basic_consume(queue=queue_name, on_message_callback=on_request)

    print(" [x] Awaiting RPC requests")
    channel.start_consuming()


execute_engine(binding_keys=["change_format", "convert"])













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
