import time

import pika

class Worker:
    def __init__(self, binding_key):
        self.binding_key = binding_key
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue

        self.channel.queue_bind(exchange='direct_logs', queue=self.queue_name, routing_key=binding_key)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback)

    def callback(self, ch, method, properties, body):
        response = body.decode()
        response = response + " from worker-2"
        print(f'Received message "{response}" with binding key "{self.binding_key}"')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        time.sleep(10)
        self.send_response(response, properties.reply_to, properties.correlation_id)

    def send_response(self, message, callback_queue, correlation_id):
        self.channel.basic_publish(
            exchange='',
            routing_key=callback_queue,
            properties=pika.BasicProperties(
                correlation_id=correlation_id,
            ),
            body=message)

        print(f'Sent response "{message}" to callback queue "{callback_queue}" with correlation ID "{correlation_id}"')

binding_key = 'key2' # replace with the binding key that you want
worker = Worker(binding_key)
print(f'Started worker for binding key "{binding_key}"')
worker.channel.start_consuming()
