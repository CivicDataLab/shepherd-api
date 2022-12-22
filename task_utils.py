from prefect import get_run_logger
import json
import uuid

import pika


def get_task_names(task_obj_list):
    task_names = []
    for obj in task_obj_list:
        task_names.append(obj.task_name)
    return task_names


def get_task_contexts(task_obj_list):
    contexts = []
    for obj in task_obj_list:
        context = json.loads(obj.context.replace('\'', '"'))
        contexts.append(context)
    return contexts


def set_task_model_values(task, pipeline):
    task.output_id = '1'
    # create_resource(
    #     {'package_id': pipeline.model.output_id, 'resource_name': task.task_name, 'data': pipeline.data})
    print({'package_id': task.output_id, 'resource_name': task.task_name, 'data': pipeline.data})
    task.status = "Done"
    task.save()


def remove_unnamed_col(data_frame):
    return data_frame.loc[:, ~data_frame.columns.str.contains('^Unnamed')]



def populate_task_schema(key_entry, format_entry, description_entry):
    schema_dict = {"key": key_entry, "format": format_entry, "description": description_entry}
    return schema_dict


def send_error_to_prefect_cloud(e: Exception):
    prefect_logger = get_run_logger()
    prefect_logger.error(str(e))


class TasksRpcClient(object):

    def __init__(self, task_name, context, data):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.routing_key = task_name
        self.context = context
        self.data = data
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
        result = self.channel.queue_declare(queue='', exclusive=False, durable=True)
        self.callback_queue = result.method.queue
        print("queue name-----", self.callback_queue)

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        print("correlation id while receiving...", props.correlation_id)
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self):
        self.response = None
        self.corr_id = str(uuid.uuid4())

        message = {"context": self.context,
                   "data": self.data}
        self.channel.basic_publish(
            exchange='topic_logs',
            routing_key=self.routing_key,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(message))
        self.connection.process_data_events(time_limit=None)
        return self.response


def publish_task_and_process_result(task_obj, context, data):
    if data is not None: # if scraper task then data will be None
        data = data.to_json()
    task_publisher = TasksRpcClient(task_obj.task_name, context, data)
    try:
        data_bytes = task_publisher.call()  # this will be a csv of bytes type
        data = str(data_bytes.decode("utf-8"))
        print(data, "******")
    except Exception as e:
        print(str(e), "&&&&&&&")
        send_error_to_prefect_cloud(e)
    print("data in prefect..", data)
    exception_flag = False
    if data.startswith("Worker failed with an error -"):
        print("found err msg", data)
        send_error_to_prefect_cloud(Exception(data))
        task_obj.status = "Failed"
        task_obj.save()
        exception_flag = True
    return data, exception_flag
