import json
import queue
import uuid

import pandas as pd
import pika
import requests
from background_task import background
from django.http import JsonResponse

from datatransform.models import Pipeline
import graphql_service
from configparser import ConfigParser
import os

config = ConfigParser()

config.read("config.ini")

data_download_url = os.environ.get('DATA_DOWNLOAD_URL', config.get("datapipeline", "DATA_DOWNLOAD_URL"))
rabbit_mq_host = os.environ.get('RABBIT_MQ_HOST', config.get("datapipeline", "RABBIT_MQ_HOST"))


@background(queue="create_pipeline")
def create_pipeline(post_data, pipeline_name):
    """ Asynchronous task to create pipeline using the request received from the API """
    p = Pipeline(status="Requested", pipeline_name=pipeline_name)
    p.save()

    p_id = p.pk

    print("inside bg task - pipeline create")
    transformers_list = post_data.get('transformers_list', None)
    res_id = post_data.get('res_id', None)
    db_action = post_data.get('db_action', None)
    data_url = data_download_url + str(res_id)
    response = graphql_service.resource_query(res_id)
    print(response)
    dataset_id = response['data']['resource']['dataset']['id']
    transformers_list = [i for i in transformers_list if i]
    try:
        data = read_data(data_url)
        p = Pipeline.objects.get(pk=p_id)
        p.status = "Created"
        p.dataset_id = dataset_id
        p.resource_id = res_id
        p.save()
    except Exception as e:
        data = None

    for _, each in enumerate(transformers_list):
        task_name = each.get('name', None)
        task_order_no = each.get('order_no', None)
        task_context = each.get('context', None)
        p.task_set.create(task_name=task_name, status="Created", order_no=task_order_no, context=task_context)

    temp_file_name = uuid.uuid4().hex
    if not data.empty:
        data.to_pickle(temp_file_name)
    message_body = {
        'p_id': p_id,
        'temp_file_name': temp_file_name,
        'res_details': response,
        'db_action': db_action
    }
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbit_mq_host))
    channel = connection.channel()

    channel.queue_declare(queue='pipeline_ui_queue')
    channel.basic_publish(exchange='',
                          routing_key='pipeline_ui_queue',
                          body=json.dumps(message_body))
    print(" [x] Sent %r" % message_body)
    connection.close()


def read_data(data_url):
    all_data = pd.read_csv(data_url)
    all_data.fillna(value="", inplace=True)

    return all_data