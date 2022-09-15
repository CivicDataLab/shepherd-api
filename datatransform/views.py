from io import StringIO

import pika
import requests

from .models import Task, Pipeline

# Create your views here.

from django.http import JsonResponse, HttpResponse
import pandas as pd
import json
import uuid


def transformer_list(request):
    transformers = [
        {"name": "skip_column", "context": [
            {"name": "columns", "type": "string", "desc": "Please enter comma separated column names to be deleted"}]},
        {"name": "merge_columns", "context": [
            {"name": "column1", "type": "string", "desc": "Please enter first column name"},
            {"name": "column2", "type": "string", "desc": "Please enter second column name"},
            {"name": "output_column", "type": "string", "desc": "Please enter output column name"},
            {"name": "separator", "type": "string", "desc": "Please enter separator char/string"}
        ]},
        {"name": "change_format", "context": [
            {"name": "format", "type": "string", "desc": "PDF/JSON/XML"}]},
        {"name": "anonymize", "context": [
            {"name": "to_replace", "type": "string", "desc": "String to be replaced"},
            {"name": "replace_val", "type": "string", "desc": "Replacement string"},
            {"name": "column", "type": "string", "desc": "Desired column"}
        ]},
        {"name": "aggregate", "context": [
            {"name": "index", "type": "string", "desc": "Field that is needed as index"},
            {"name": "columns", "type": "string", "desc": "CSV of column names"},
            {"name": "values", "type": "string", "desc": "values"}
        ]},
        {"name": "query_data_resource", "context": [
            {"name": "columns", "type": "string", "desc": "Enter csv columns to be queried"},
            {"name": "rows", "type": "string", "desc": "Enter num of rows required"}
        ]}
    ]

    context = {"result": transformers, "Success": True}
    return JsonResponse(context, safe=False)


def pipeline_filter(request):
    dataset_id = request.GET.get('datasetId', None)
    pipeline_data = list(Pipeline.objects.filter(dataset_id=dataset_id))
    resp_list = []
    for each in pipeline_data:
        data = {'pipeline_id': each.pipeline_id, 'pipeline_name': each.pipeline_name,
                'output_id': each.output_id, 'created_at': each.created_at,
                'status': each.status, 'resource_id': each.resource_id
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
        print("#####",post_data)
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
        db_action = post_data.get('db_action', None)
        pipeline_name = post_data.get('pipeline_name', None)
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
        request = requests.post('http://idpbe.civicdatalab.in/graphql', json={'query': query}, headers=headers)
        response = json.loads(request.text)
        print(response)
        dataset_id = response['data']['resource']['dataset']['id']
        #data_url = response['data']['resource']['remote_url']
        print(data_url)
        transformers_list = [i for i in transformers_list if i]
        try:
            data = read_data(data_url)
            # response = requests.get(data_url)
            # data_txt = response.text
            # data = pd.read_csv(StringIO(data_txt), sep=",")
            # print(data)
        except Exception as e:
            data = None
            context = {"result": "NO data!!", "Success": False}
            return JsonResponse(context, safe=False)
        p = Pipeline(status="Created", pipeline_name=pipeline_name, dataset_id=dataset_id, resource_id=res_id)

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
            'db_action' : db_action
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

# def multiply(data, trans_column, trans_operval):

#     data[trans_column] = data[trans_column].apply(lambda x: x*trans_operval)
#     return data


# def add(data, trans_column, trans_operval):

#     data[trans_column] = data[trans_column].apply(lambda x: x+trans_operval)
#     return data
