from django.shortcuts import render
from .models import Task, Pipeline

# Create your views here.

from django.http import HttpResponse
from django.http import JsonResponse
import pandas as pd
import json


def transformer_list(request):

    transformers = [
                    {"name" : "skip_column", "context":  [{"name":"column", "type":"string", "desc":"Please enter comma separated column names to be deleted"}]},
                    {"name" : "merge_columns", "context": [
                                                                {"name":"column1", "type":"string", "desc":"Please enter first column name"}, 
                                                                {"name":"column2", "type":"string", "desc":"Please enter second column name"},
                                                                {"name":"output_column", "type":"string", "desc":"Please enter output column name"}
                                                          ]}
                    ]

    context = {"result" : transformers, "Success": True}
        
    return JsonResponse(context, safe=False)

def pipe_list(request):

    task_data  = list(Task.objects.all().values())

    data = {}
    for each in task_data:
        if each['Pipeline_id_id'] not in data:
            data[each['Pipeline_id_id']] = {'date': each['created_at'], 'pipeline':[{"name":each['task_name'], "step": each['order_no'], "status":each['status'], "result":each['result_url']}]}
        else: 
            data[each['Pipeline_id_id']]['pipeline'].append({"name":each['task_name'], "step": each['order_no'], "status":each['status'], "result":each['result_url']})


    context = {"result" : data, "Success": True}

    return JsonResponse(context, safe=False)


def pipe_create(request):
    if request.method == 'POST':

        print ("enter")
        print (request.body)

        post_data = json.loads(request.body.decode('utf-8'))
        print (post_data)
        transformers_list =  post_data.get('transformers_list', None)
        data_url          =  post_data.get('data_url', None)

        print(data_url, transformers_list)
        # data = read_data(data_url)
        transformers_list = [i for i in transformers_list if i]

        


        p = Pipeline(status="started")
        p.save()

        p_id = p.pk        

        
        for index, each in enumerate(transformers_list):
            task_name      = each.get('name', None)
            task_order_no  = each.get('order_no', None)
            task_context   = each.get('context', None)


            p = Pipeline.objects.get(pk=p_id)
            t = p.task_set.create(task_name=task_name, status="Not Started", order_no=task_order_no, context=task_context)

        context = {"result" : p_id, "Success": True}
        return JsonResponse(context, safe=False)

 
        # transformed_data = data.copy()
        # all_stage_data = {0:transformed_data}
   
        # transformed_data = globals()[trans_oper](transformed_data, trans_column, trans_operval)
        # all_stage_data[index+1] = transformed_data


def read_data(data_url):

    all_data    = pd.read_csv(data_url)
    all_data.fillna(value="", inplace=True)

    return all_data


# def multiply(data, trans_column, trans_operval):

#     data[trans_column] = data[trans_column].apply(lambda x: x*trans_operval)
#     return data


# def add(data, trans_column, trans_operval):

#     data[trans_column] = data[trans_column].apply(lambda x: x+trans_operval)
#     return data





