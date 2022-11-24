import json
import queue
import uuid

import pandas as pd
import pika
import requests
from background_task import background
from django.http import JsonResponse

import log_utils
from datatransform.models import Pipeline
import graphql_service
from configparser import ConfigParser
import os

config = ConfigParser()

config.read("config.ini")

data_download_url = os.environ.get('DATA_DOWNLOAD_URL', config.get("datapipeline", "DATA_DOWNLOAD_URL"))
rabbit_mq_host = os.environ.get('RABBIT_MQ_HOST', config.get("datapipeline", "RABBIT_MQ_HOST"))


@background(queue="create_pipeline")
def create_pipeline(post_data, p_id):
    """ Asynchronous task to create pipeline using the request received from the API """
    p = Pipeline.objects.get(pk=p_id)
    pipeline_name = getattr(p, "pipeline_name")

    logger = log_utils.get_logger_for_existing_file(p_id)

    res_id = post_data.get('res_id', None)
    dataset_id = post_data.get('dataset_id', None)
    p.dataset_id = dataset_id
    p.save()
    db_action = post_data.get('db_action', None)
    data_url = data_download_url + str(res_id)
    try:
        response = graphql_service.resource_query(res_id)
        print(response)
    except Exception as e:
        logger.error(f"ERROR: couldn't fetch response from graphql. Got an Exception - {str(e)}")
        p.err_msg = str(f"ERROR: couldn't fetch response from graphql. Got an Exception - {str(e)}")
        p.save()
    file_format = response['data']['resource']['file_details']['format']
    if file_format == "CSV":
        try:
            data = read_data(data_url)
            p = Pipeline.objects.get(pk=p_id)
            p.status = "Created"
            logger.info(f"INFO: Pipeline created")
            p.resource_identifier = res_id
            p.save()
            temp_file_name = uuid.uuid4().hex
            if not data.empty:
                data.to_pickle(temp_file_name)
        except Exception as e:
            data = None
            p.status = "Failed"
            p.err_msg = str(e)
            p.save()
    elif file_format == "JSON":
        try:
            data = read_json_data(data_url)
            p = Pipeline.objects.get(pk=p_id)
            p.status = "Created"
            logger.info(f"INFO: Pipeline created")
            p.resource_identifier = res_id
            p.save()
            temp_file_name = uuid.uuid4().hex + ".json"
            json_object = json.dumps(data, indent=4)
            with open(temp_file_name, "w") as outfile:
                outfile.write(json_object)

        except Exception as e:
            data = None
            p.status = "Failed"
            p.err_msg = str(e)
            p.save()


    message_body = {
        'p_id': p_id,
        'temp_file_name': temp_file_name,
        'res_details': response,
        'db_action': db_action,
        'file_format': file_format
    }
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbit_mq_host))
    channel = connection.channel()

    channel.queue_declare(queue='pipeline_ui_queue')
    channel.basic_publish(exchange='',
                          routing_key='pipeline_ui_queue',
                          body=json.dumps(message_body))
    logger.info(f"INFO: Published {message_body}")
    print(" [x] Sent %r" % message_body)
    connection.close()


def read_data(data_url):
    all_data = pd.read_csv(data_url)
    all_data.fillna(value="", inplace=True)

    return all_data

def read_json_data(data_url):
    data_response = requests.get(data_url)
    return data_response.json()
