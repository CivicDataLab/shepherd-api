import pika
import requests

from .models import Task, Pipeline

# Create your views here.

from django.http import JsonResponse
import pandas as pd
import json
import uuid
from utils import upload_dataset


def transformer_list(request):
    transformers = [
        {"name": "skip_column", "context": [
            {"name": "columns", "type": "string", "desc": "Please enter comma separated column names to be deleted"}]},
        {"name": "merge_columns", "context": [
            {"name": "column1", "type": "string", "desc": "Please enter first column name"},
            {"name": "column2", "type": "string", "desc": "Please enter second column name"},
            {"name": "output_column", "type": "string", "desc": "Please enter output column name"},
            {"name": "separator", "type": "string", "desc": "Please enter separator char/string"}
        ]}
    ]

    context = {"result": transformers, "Success": True}
    return JsonResponse(context, safe=False)


def pipe_list(request):
    task_data = list(Task.objects.all().values())

    data = {}
    for each in task_data:
        p = Pipeline.objects.get(pk=each['Pipeline_id_id'])
        res_url = "https://ndp.ckan.civicdatalab.in/dataset/" + p.output_id + "/resource/" + each['output_id']

        if each['Pipeline_id_id'] not in data:
            data[each['Pipeline_id_id']] = {'date': each['created_at'], 'status': p.status, 'name': p.pipeline_name,
                                            'pipeline': [{"name": each['task_name'], "step": each['order_no'],
                                                          "status": each['status'], "result": res_url}]}
        else:
            data[each['Pipeline_id_id']]['pipeline'].append(
                {"name": each['task_name'], "step": each['order_no'], "status": each['status'], "result": res_url})

    context = {"result": data, "Success": True}

    return JsonResponse(context, safe=False)


def pipe_create(request):
    if request.method == 'POST':

        # print("enter")
        # print(request.body)

        post_data = json.loads(request.body.decode('utf-8'))
        transformers_list = post_data.get('transformers_list', None)
        data_url = post_data.get('data_url', None)
        org_name = post_data.get('org_name', None)
        pipeline_name = post_data.get('name', '')

        # print(data_url, transformers_list)
        transformers_list = [i for i in transformers_list if i]
        try:
            data = read_data(data_url)
        except Exception as e:
            data = None

        p = Pipeline(status="Created", pipeline_name=pipeline_name)

        # p.output_id = upload_dataset(pipeline_name, org_name)
        p.save()

        p_id = p.pk

        for _, each in enumerate(transformers_list):
            task_name = each.get('name', None)
            task_order_no = each.get('order_no', None)
            task_context = each.get('context', None)

            p = Pipeline.objects.get(pk=p_id)
            p.task_set.create(task_name=task_name, status="Created", order_no=task_order_no, context=task_context)
        temp_file_name = uuid.uuid4().hex
        if not data.empty:
            data.to_pickle(temp_file_name)
        message_body = {
            'p_id': p_id,
            'temp_file_name': temp_file_name
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
        context = {"result": p_id, "Success": True}
        return JsonResponse(context, safe=False)

        # transformed_data = data.copy()
        # all_stage_data = {0:transformed_data}

        # transformed_data = globals()[trans_oper](transformed_data, trans_column, trans_operval)
        # all_stage_data[index+1] = transformed_data


def read_data(data_url):
    all_data = pd.read_csv(data_url)
    all_data.fillna(value="", inplace=True)

    return all_data


def res_transform(request):
    if request.method == 'POST':

        # print("enter")
        # print(request.body)

        post_data = json.loads(request.body.decode('utf-8'))
        transformers_list = post_data.get('transformers_list', None)
        # org_name = post_data.get('org_name', None)
        res_id = post_data.get('res_id', None)
        pipeline_name = str(res_id) + "-" + str(uuid.uuid4())

        query = """{
                    resource(resource_id: 10) {
                    id
                    title
                    description
                    issued
                    modified
                    remote_url
                    format
                    file
                    status
                    dataset {
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
                      sector
                      status
                      remark
                      funnel
                      action
                      access_type
                      geography
                      License
                    }
                  }
                }
                """
        headers = {}  # {"Authorization": "Bearer YOUR API KEY"}
        request = requests.post('http://idpbe.civicdatalab.in/graphql', json={'query': query}, headers=headers)
        response = json.loads(request.text)
        data_url = response['data']['resource']['remote_url']

        transformers_list = [i for i in transformers_list if i]
        try:
            data = read_data(data_url)
        except Exception as e:
            data = None

        p = Pipeline(status="Created", pipeline_name=pipeline_name)

        # p.output_id = upload_dataset(pipeline_name, org_name)
        p.save()

        p_id = p.pk

        for _, each in enumerate(transformers_list):
            task_name = each.get('name', None)
            task_order_no = each.get('order_no', None)
            task_context = each.get('context', None)

            p = Pipeline.objects.get(pk=p_id)
            p.task_set.create(task_name=task_name, status="Created", order_no=task_order_no, context=task_context)
        temp_file_name = uuid.uuid4().hex
        if not data.empty:
            data.to_pickle(temp_file_name)
        message_body = {
            'p_id': p_id,
            'temp_file_name': temp_file_name,
            'res_details': response,
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
        context = {"result": p_id, "Success": True}
        return JsonResponse(context, safe=False)


# def multiply(data, trans_column, trans_operval):

#     data[trans_column] = data[trans_column].apply(lambda x: x*trans_operval)
#     return data


# def add(data, trans_column, trans_operval):

#     data[trans_column] = data[trans_column].apply(lambda x: x+trans_operval)
#     return data
