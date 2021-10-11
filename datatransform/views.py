from .models import Task, Pipeline
from pipeline.model_to_pipeline import model_to_pipeline

# Create your views here.

from django.http import JsonResponse
import pandas as pd
import json
import uuid
from utils import upload_dataset


def transformer_list(request):
    transformers = [{"name": "skip_column", "context": {"columns": "list_of_strings"}},
                    {"name": "merge_columns",
                     "context": {"column1": "string", "column2": "string", "output_column": "string"}}]

    transformers = [
                    {"name" : "skip_column", "context":  [{"name":"column", "type":"string", "desc":"Please enter comma separated column names to be deleted"}]},
                    {"name" : "merge_columns", "context": [
                                                                {"name":"column1", "type":"string", "desc":"Please enter first column name"}, 
                                                                {"name":"column2", "type":"string", "desc":"Please enter second column name"},
                                                                {"name":"output_column", "type":"string", "desc":"Please enter output column name"}
                                                          ]}
                    ]

    context = {"result": transformers, "Success": True}
    return JsonResponse(context, safe=False)

def pipe_list(request):
    
    task_data  = list(Task.objects.all().values())

    data = {}
    for each in task_data:
        if each['Pipeline_id_id'] not in data:
            p = Pipeline.objects.get(pk=each['Pipeline_id_id'])
            data[each['Pipeline_id_id']] = {'date': each['created_at'], 'status': p.status, 'pipeline':[{"name":each['task_name'], "step": each['order_no'], "status":each['status'], "result":each['result_url']}]}
        else: 
            data[each['Pipeline_id_id']]['pipeline'].append({"name":each['task_name'], "step": each['order_no'], "status":each['status'], "result":each['result_url']})
        
        

         
    context = {"result" : data, "Success": True}

    return JsonResponse(context, safe=False)


def pipe_create(request):
    if request.method == 'POST':

        # print("enter")
        # print(request.body)

        post_data = json.loads(request.body.decode('utf-8'))
        # print(post_data)
        transformers_list = post_data.get('transformers_list', None)
        data_url = post_data.get('data_url', None)
        pipeline_name = post_data.get('name', '')

        # print(data_url, transformers_list)
        data = read_data(data_url)
        transformers_list = [i for i in transformers_list if i]

        p = Pipeline(status="Created", pipeline_name=pipeline_name)

        p.output_id = upload_dataset(pipeline_name)
        p.save()

        p_id = p.pk

        for _, each in enumerate(transformers_list):
            task_name = each.get('name', None)
            task_order_no = each.get('order_no', None)
            task_context = each.get('context', None)

            p = Pipeline.objects.get(pk=p_id)
            p.task_set.create(task_name=task_name, status="Created", order_no=task_order_no, context=task_context)
        temp_file_name = uuid.uuid4().hex
        data.to_pickle(temp_file_name)
        model_to_pipeline(p_id, temp_file_name)
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

# def multiply(data, trans_column, trans_operval):

#     data[trans_column] = data[trans_column].apply(lambda x: x*trans_operval)
#     return data


# def add(data, trans_column, trans_operval):

#     data[trans_column] = data[trans_column].apply(lambda x: x+trans_operval)
#     return data
