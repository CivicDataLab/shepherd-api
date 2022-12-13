import pika
import requests
from background_task.models import CompletedTask

from .models import Task, Pipeline
# Create your views here.

from django.http import JsonResponse, HttpResponse
import pandas as pd
import json
import uuid


def transformer_list(request):
    #field_single
    # field_multi
    transformers = [
        {"name": "skip_column", "context": [
            {"name": "columns", "type": "field_multi", "desc": "Please select column names to be deleted"}]},
        {"name": "merge_columns", "context": [
            {"name": "column1", "type": "field_single", "desc": "Please select first column name"},
            {"name": "column2", "type": "field_single", "desc": "Please select second column name"},
            {"name": "output_column", "type": "string", "desc": "Please enter output column name"},
            {"name": "separator", "type": "string", "desc": "Please enter separator char/string"}
        ]},
        {"name": "change_format", "context": [
            {"name": "format", "type": "string", "desc": "xml/json/pdf"}]},
        {"name": "anonymize", "context": [
            {"name": "to_replace", "type": "string", "desc": "String to be replaced"},
            {"name": "replace_val", "type": "string", "desc": "Replacement string"},
            {"name": "column", "type": "field_single", "desc": "Please select column name to perform operation"}
        ]},
        {"name": "aggregate", "context": [
            {"name": "index", "type": "string", "desc": "Field that is needed as index"},
            {"name": "columns", "type": "field_multi", "desc": "Select column names"},
            {"name": "values", "type": "string", "desc": "values"}
        ]}
    ]

    context = {"result": transformers, "Success": True}
    return JsonResponse(context, safe=False)


def pipeline_filter(request):
    dataset_id = request.GET.get('datasetId', None)
    pipeline_data = list(Pipeline.objects.filter(dataset_id=dataset_id))
    resp_list = []
    for each in pipeline_data:
        p_id = each.pipeline_id
        task_data = list(Task.objects.filter(Pipeline_id=p_id))
        tasks_list = []
        for task in task_data:
            t_data = {
                'task_id': task.task_id, 'task_name': task.task_name, 'context': task.context,
                'status': task.status, 'order_no': task.order_no, 'created_at': task.created_at,
                'result_url': task.result_url, 'output_id': task.output_id
            }
            tasks_list.append(t_data)
        data = {'pipeline_id': each.pipeline_id, 'pipeline_name': each.pipeline_name,
                'output_id': each.output_id, 'created_at': each.created_at,
                'status': each.status, 'resource_id': each.resource_id, 'tasks': tasks_list
                }
        resp_list.append(data)

    context = {"result": resp_list, "Success": True}

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
        post_data = json.loads(request.body.decode('utf-8'))
        print("#####", post_data)
        transformers_list = post_data.get('transformers_list', None)
        data_url = post_data.get('data_url', None)
        pipeline_name = post_data.get('pipeline_name', '')

        transformers_list = [i for i in transformers_list if i]
        try:
            data = read_data(data_url)
        except Exception as e:
            data = None

        p = Pipeline(status="Created", pipeline_name=pipeline_name)

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
            'res_details': ""
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


def read_data(data_url):
    all_data = pd.read_csv(data_url)

    return all_data
