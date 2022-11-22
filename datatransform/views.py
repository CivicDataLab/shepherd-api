import pika
import requests
from background_task.models import CompletedTask

from pipeline.api_resource_query_bg import api_resource_query_task
import pipeline_creator_bg
from .models import Task, Pipeline
# Create your views here.

from django.http import JsonResponse, HttpResponse
import pandas as pd
import json
import uuid


def transformer_list(request):
    transformers = [
        {"name": "skip_column", "context": [
            {"name": "columns", "type": "field_multi", "desc": "Please select column names to be deleted"}]},
        {"name": "merge_columns", "context": [
            {"name": "column1", "type": "field_single", "desc": "Please select first column name"},
            {"name": "column2", "type": "field_single", "desc": "Please select second column name"},
            {"name": "output_column", "type": "string", "desc": "Please enter output column name"},
            {"name": "separator", "type": "string", "desc": "Please enter separator char/string"},
            {"name": "retain_cols", "type": "boolean", "desc": "Whether top retain the merged columns"}
        ]},
        {"name": "change_format", "context": [
            {"name": "format", "type": "string", "desc": "xml/json/pdf"}]},
        {"name": "anonymize", "context": [
            {"name": "column", "type": "field_single", "desc": "Please select column name to perform operation"},
            {"name": "option", "type": "option_single", "desc": "Choose an option to anonymize"},
            {"name": "special_char", "type": "special_char_single", "desc": "Choose a special character"},
            {"name": "n", "type": "n_type", "desc": "Please enter value of n"}
        ]},
        {"name": "aggregate", "context": [
            {"name": "index", "type": "field_multi", "desc": "Fields which need to be grouped"},
            {"name": "columns", "type": "field_multi", "desc": "Reference fields for aggregation"},
            {"name": "values", "type": "field_multi", "desc": "Fields for which the aggregated counts to be extracted"}
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
                'status': each.status, 'resource_id': each.resource_identifier, 'tasks': tasks_list
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

        # print("enter")
        # print(request.body)

        post_data = json.loads(request.body.decode('utf-8'))
        print("#####", post_data)
        transformers_list = post_data.get('transformers_list', None)
        data_url = post_data.get('data_url', None)
        org_name = post_data.get('org_name', None)
        pipeline_name = post_data.get('name', '')
        print("*******", transformers_list)
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
    all_data.fillna(value="", inplace=True)

    return all_data


def res_transform(request):
    """ Triggers pipeline_creator_bg background task. """
    if request.method == 'POST':
        post_data = json.loads(request.body.decode('utf-8'))
        pipeline_name = post_data.get('pipeline_name', None)
        pipeline_creator_bg.create_pipeline(post_data, pipeline_name)
        completed_tasks_qs = CompletedTask.objects.all()
        print(completed_tasks_qs)
        context = {"result": pipeline_name, "Success": True}
        return JsonResponse(context, safe=False)


def api_res_transform(request):
    # save the pipeline data against api resource id
    if request.method == 'POST':
        post_data = json.loads(request.body.decode('utf-8'))
        api_source_id = post_data.get('api_source_id', None)
        dataset_id = post_data.get('dataset_id', None)
        transformers_list = post_data.get('transformers_list', None)
        transformers_list = [i for i in transformers_list if i]
        pipeline_name = post_data.get('pipeline_name', None)
        p = Pipeline(status="Created", pipeline_name=pipeline_name, resource_identifier=str(api_source_id), dataset_id=dataset_id)
        print(api_source_id)
        print("***", p.resource_identifier)
        p.save()
        p_id = p.pk

        for _, each in enumerate(transformers_list):
            task_name = each.get('name', None)
            task_order_no = each.get('order_no', None)
            task_context = each.get('context', None)
            p.task_set.create(task_name=task_name, status="Created", order_no=task_order_no, context=task_context)
        p.save()

        context = {"result": p_id, "Success": True}
        return JsonResponse(context, safe=False)


def api_source_query(request):
    if request.method == 'POST':
        post_data = json.loads(request.body.decode('utf-8'))
        api_source_id = str(post_data.get('api_source_id', None))
        request_id = post_data.get('request_id', None)
        request_columns = post_data.get('request_columns', "")
        request_rows = post_data.get('request_rows', "")
        try:
            pipeline_object = list(Pipeline.objects.filter(resource_identifier=api_source_id))[-1]
            p_id = getattr(pipeline_object, "pipeline_id")
        except Exception as e:
            print(str(e))
            p_id = None
        api_resource_query_task(p_id, api_source_id, request_id, request_columns, request_rows)

        context = {"result": p_id, "Success": True}
        return JsonResponse(context, safe=False)



def custom_data_viewer(request):
    if request.method == 'POST':
        post_data = json.loads(request.body.decode('utf-8'))
        print("data received..", post_data)
        res_id = post_data.get('res_id')
        columns = post_data.get('columns')
        num_rows = post_data.get('rows')
        # print(data_url, columns, num_rows)
        data_url = "http://idpbe.civicdatalab.in/download/" + str(res_id)
        try:
            data = read_data(data_url)
        except Exception as e:
            data = None
        # if no columns specified, return whole dataframe
        if len(columns) == 0:
            column_selected_df = data
        else:
            column_selected_df = data.loc[:, data.columns.isin(columns)]
        # if row length is not specified return all rows
        if num_rows == "" or int(num_rows) > len(column_selected_df):
            final_df = column_selected_df
        else:
            num_rows_int = int(num_rows)
            final_df = column_selected_df.iloc[:num_rows_int]

        print(final_df)
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename=export.csv'
        final_df.to_csv(path_or_buf=response)  # with other applicable parameters
        return response
