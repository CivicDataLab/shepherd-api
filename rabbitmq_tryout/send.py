import json
import uuid

import pika
import sys


class FibonacciRpcClient(object):

    def __init__(self, task_name, context):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.routing_key = task_name
        self.context = context
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        print("queue name-----", self.callback_queue)
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())

        message = {"context": self.context}

        self.channel.basic_publish(
            exchange='logs_topic',
            routing_key=self.routing_key,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(message))
        self.connection.process_data_events(time_limit=None)
        return int(self.response)

fibonacci_rpc = FibonacciRpcClient("skip_column", {"col":"asdsadasd"})

print(" [x] Requesting fib(30)")
response = fibonacci_rpc.call(30)
print(" [.] Got %r" % response)









#
# def task_publisher(task_name, context):
#     connection = pika.BlockingConnection(
#         pika.ConnectionParameters(host='localhost'))
#     channel = connection.channel()
#
#     channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
#
#     routing_key = task_name
#     context = context
#     message = json.dumps(context)
#     channel.basic_publish(
#         exchange='logs_topic', routing_key=routing_key, body=message)
#     print(" [x] Sent %r:%r" % (routing_key, message))
#     connection.close()
#
# if __name__ == "__main__":
#     task_publisher("skip_column", {"columns": ["price"]})
#     task_publisher("merge_column", {"abc": "sdlakjfdsdjf"})
#     task_publisher("ok_dookie", {"Adsad":4})
#
