import pika
import uuid

class Publisher:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='direct_logs', exchange_type='direct')
        self.channel.basic_qos(prefetch_count=1)

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(queue=self.callback_queue, on_message_callback=self.on_response)

    def on_response(self, ch, method, props, body):
        if self.correlation_id == props.correlation_id:
            print("matched!!")
            self.response = body

    def call(self, message, binding_key):
        self.response = None
        self.correlation_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='direct_logs',
            routing_key=binding_key,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.correlation_id,
            ),
            body=message)

        timer = 0
        while self.response is None and timer < 2:
            self.connection.process_data_events(time_limit=2)
            timer += 1

        if self.response is None:
            print(f"No worker available for message '{message}' with binding key '{binding_key}'. Deleting message from queue.")
            self.channel.queue_purge(self.callback_queue)
            return "No worker bro"

        return self.response.decode()

publisher = Publisher()

binding_key = 'key1' # replace with the binding key that you want
message = 'Hello World!' # replace with the message that you want to send

response = publisher.call(message, binding_key)
print(f'Response: {response}')
