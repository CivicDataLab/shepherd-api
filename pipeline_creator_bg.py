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
import shutil

config = ConfigParser()

config.read("config.ini")

data_download_url = os.environ.get('DATA_DOWNLOAD_URL', config.get("datapipeline", "DATA_DOWNLOAD_URL"))
rabbit_mq_host = os.environ.get('RABBIT_MQ_HOST', config.get("datapipeline", "RABBIT_MQ_HOST"))


#@background(queue="create_pipeline")
def create_pipeline(post_data, p_id):
    """ Asynchronous task to create pipeline using the request received from the API """
    p = Pipeline.objects.get(pk=p_id)
    pipeline_name = getattr(p, "pipeline_name")

    logger = log_utils.get_logger_for_existing_file(p_id)

    res_id = post_data.get('res_id', None)
    dataset_id = post_data.get('dataset_id', None)
    new_res_name = post_data.get('new_res_name', None)
    p.dataset_id = dataset_id
    p.save()
    db_action = post_data.get('db_action', None)
    data_url = data_download_url + str(res_id)
    file_format = "" # initial file format of the resource.
    temp_file_name = uuid.uuid4().hex
    try:
        response = graphql_service.resource_query(res_id)
        print(response)
        file_format = response['data']['resource']['file_details']['format']
    except Exception as e:
        logger.error(f"ERROR: couldn't fetch response from graphql. Got an Exception - {str(e)}")
        p.err_msg = str(f"ERROR: couldn't fetch response from graphql. Got an Exception - {str(e)}")
        p.save()
    print("###", file_format)
    if file_format.lower() == "csv":
        try:
            data = read_data(data_url)
            p = Pipeline.objects.get(pk=p_id)
            p.status = "Created"
            logger.info(f"INFO: Pipeline created")
            p.resource_identifier = res_id
            p.save()
            if not data.empty:
                data.to_csv(temp_file_name)
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
            temp_file_name = temp_file_name + ".json"
            json_object = json.dumps(data, indent=4)
            with open(temp_file_name, "w") as outfile:
                outfile.write(json_object)

        except Exception as e:
            data = None
            p.status = "Failed"
            p.err_msg = str(e)
            p.save()
    # if to create a new res, create a copy of existing resource
    if db_action == "create":
        if new_res_name is not None:
            resource_name = new_res_name
        else:
            resource_name = response['data']['resource']['title'] +  "-" + (str(uuid.uuid4().hex)[0:5])
        #
        description = response['data']['resource']['description']
        schema = response['data']['resource']['schema']
        schema1= []
        for each in schema:
            del each['id']
            schema1.append(each)
        schema = json.dumps(schema1)
        schema = schema.replace('"id":', 'id:').replace('"key":', 'key:').replace('"format":', 'format:').replace(
            '"description":', 'description:')
        org_id = response['data']['resource']['dataset']['catalog']['organization']['id']
        file_path = resource_name + "." + str(file_format).lower()
        # copying the data to the file with same title as resource
        shutil.copy(temp_file_name, file_path)
        files = [
            ('0', (file_path, open(file_path, "rb"), str(file_format).lower()))
        ]
        print(files)
        try:
            create_resource_response = graphql_service.create_resource(resource_name,
                                                     description,
                                                     schema,
                                                     file_format,
                                                     files, org_id, dataset_id)
            p.resultant_res_id = create_resource_response['data']['create_resource']['resource']['id']
            p.save()
            print("Created a copy at----", p.resultant_res_id)
            # assign res_id of response - the value of new_res id as update_resource uses this id
            # for later steps.
            response['data']['resource']['id'] = p.resultant_res_id
        except Exception as e:
            logger.error(f"ERROR: Got an error while creating a copy of the given resorce"
                         f"{res_id} - {str(e)}")
        finally:
            files[0][1][1].close()
            # os.remove(file_path)
    else:
        pass
    pipe_action = post_data.get('pipe_action')
    #  Run transformations only when the pipe_action is update. Else need not run this.
    if pipe_action == "update":
        print ('****------------------', file_format)
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
