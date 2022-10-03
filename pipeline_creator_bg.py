import json
import queue
import uuid

import pandas as pd
import pika
import requests
from background_task import background
from django.http import JsonResponse

from datatransform.models import Pipeline


@background(queue="create_pipeline")
def create_pipeline(post_data, pipeline_name):
    """ Asynchronous task to create pipeline using the request received from the API """
    p = Pipeline(status="Requested", pipeline_name=pipeline_name)
    p.save()

    p_id = p.pk

    print("inside bg task - pipeline create")
    transformers_list = post_data.get('transformers_list', None)
    # org_name = post_data.get('org_name', None)
    res_id = post_data.get('res_id', None)
    db_action = post_data.get('db_action', None)
    data_url = "http://idpbe.civicdatalab.in/download/" + str(res_id)
    # data_url = "https://justicehub.in/dataset/a1d29ace-784b-4479-af09-11aea7be1bf5/resource/0e5974a1-d66d-40f8-85a4-750adc470f26/download/metadata.csv"
    query = f"""{{
                       resource(resource_id: {res_id}) {{
                       id
                       title
                       description
                       issued
                       modified
                       remote_url
                       format
                       schema{{
                       id
                       key
                       format
                       description
                       }}
                       file
                       status
                       dataset {{
                         id
                         title
                         description
                         issued
                         remote_issued
                         remote_modified
                         period_from
                         period_to
                         update_frequency
                         modified
                         status
                         remark
                         funnel
                         action
                         access_type
                         License
                       }}
                     }}
                   }}
                   """
    headers = {}  # {"Authorization": "Bearer YOUR API KEY"}
    request = requests.post('https://idpbe.civicdatalab.in/graphql', json={'query': query}, headers=headers)
    response = json.loads(request.text)
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
        # response = requests.get(data_url)
        # data_txt = response.text
        # data = pd.read_csv(StringIO(data_txt), sep=",")
        # print(data)
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
        pika.ConnectionParameters(host='localhost'))
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
