import pika
import requests
from background_task.models import CompletedTask

import log_utils
from pipeline.api_resource_query_bg import api_resource_query_task
from pipeline.api_res_run_transform_task import api_res_run_transform_task
import pipeline_creator_bg
from .models import Task, Pipeline

# Create your views here.

from django.http import JsonResponse, HttpResponse
import pandas as pd
import json
import uuid


def api_transformer_list(request):
    """ returns list of transformations for api based resources with
    required input fields to the front-end """
    transformers = [
        {
            "name": "skip_column",
            "context": [
                {
                    "name": "columns",
                    "type": "field_multi",
                    "desc": "Please select column names to be deleted",
                }
            ],
        },
        {
            "name": "anonymize",
            "context": [
                {
                    "name": "column",
                    "type": "field_single",
                    "desc": "Please select column name to perform operation",
                },
                {
                    "name": "option",
                    "type": "option_single",
                    "desc": "Choose an option to anonymize",
                },
                {
                    "name": "special_char",
                    "type": "special_char_single",
                    "desc": "Choose a special character",
                },
                {"name": "n", "type": "n_type", "desc": "Please enter the value of n"},
            ],
        },
    ]
    context = {"result": transformers, "Success": True}
    return JsonResponse(context, safe=False)


def transformer_list(request):
    """ returns list of transformations with required input fields to the front-end """
    transformers = [
        {
            "name": "skip_column",
            "context": [
                {
                    "name": "columns",
                    "type": "field_multi",
                    "desc": "Please select column names to be deleted",
                }
            ],
        },
        {
            "name": "merge_columns",
            "context": [
                {
                    "name": "column1",
                    "type": "field_single",
                    "desc": "Please select first column name",
                },
                {
                    "name": "column2",
                    "type": "field_single",
                    "desc": "Please select second column name",
                },
                {
                    "name": "output_column",
                    "type": "string",
                    "desc": "Please enter output column name",
                },
                {
                    "name": "separator",
                    "type": "string",
                    "desc": "Please enter separator char/string",
                },
                {
                    "name": "retain_cols",
                    "type": "boolean",
                    "desc": "Whether top retain the merged columns",
                },
            ],
        },
        {
            "name": "change_format_to_pdf",
            "context": [{"name": "format", "type": "formatfield_single",
                         "desc": "Change the format to PDF"}],
        },
        {
            "name": "anonymize",
            "context": [
                {
                    "name": "column",
                    "type": "field_single",
                    "desc": "Please select column name to perform operation",
                },
                {
                    "name": "option",
                    "type": "option_single",
                    "desc": "Choose an option to anonymize",
                },
                {
                    "name": "special_char",
                    "type": "special_char_single",
                    "desc": "Choose a special character",
                },
                {"name": "n", "type": "n_type", "desc": "Please enter the value of n"},
            ],
        },
        {
            "name": "aggregate",
            "context": [
                {
                    "name": "index",
                    "type": "field_multi",
                    "desc": "Fields which need to be grouped",
                },
                {
                    "name": "columns",
                    "type": "field_multi",
                    "desc": "Reference fields for aggregation",
                },
                {
                    "name": "values",
                    "type": "field_multi",
                    "desc": "Fields for which the aggregated counts to be extracted",
                },
            ],
        },
    ]

    context = {"result": transformers, "Success": True}
    return JsonResponse(context, safe=False)


def pipeline_filter(request):
    """ Filters the pipeline objects based on the given dataset_id and returns all related data"""
    dataset_id = request.GET.get("datasetId", None)
    pipeline_data = list(Pipeline.objects.filter(dataset_id=dataset_id))
    resp_list = []
    for each in pipeline_data:
        p_id = each.pipeline_id
        task_data = list(Task.objects.filter(Pipeline_id=p_id))
        tasks_list = []
        for task in task_data:
            t_data = {
                "task_id": task.task_id,
                "task_name": task.task_name,
                "context": task.context,
                "status": task.status,
                "order_no": task.order_no,
                "created_at": task.created_at,
                "result_url": task.result_url,
                "output_id": task.output_id,
            }
            tasks_list.append(t_data)
        data = {
            "pipeline_id": each.pipeline_id,
            "pipeline_name": each.pipeline_name,
            "output_id": each.output_id,
            "created_at": each.created_at,
            "db_action": each.db_action,
            "status": each.status,
            "resource_id": each.resource_identifier,
            "resultant_res_id": each.resultant_res_id,
            "error_message": each.err_msg,
            "tasks": tasks_list,
        }
        resp_list.append(data)

    context = {"result": resp_list, "Success": True}

    return JsonResponse(context, safe=False)


def pipe_list(request):
    task_data = list(Task.objects.all().values())

    data = {}
    for each in task_data:
        try:
            p = Pipeline.objects.get(pk=each["Pipeline_id_id"])
        except Exception as e:
            continue
        res_url = (
                "https://ndp.ckan.civicdatalab.in/dataset/"
                + p.output_id
                + "/resource/"
                + each["output_id"]
        )

        if each["Pipeline_id_id"] not in data:
            data[each["Pipeline_id_id"]] = {
                "date": each["created_at"],
                "status": p.status,
                "name": p.pipeline_name,
                "pipeline": [
                    {
                        "name": each["task_name"],
                        "step": each["order_no"],
                        "status": each["status"],
                        "result": res_url,
                    }
                ],
            }
        else:
            data[each["Pipeline_id_id"]]["pipeline"].append(
                {
                    "name": each["task_name"],
                    "step": each["order_no"],
                    "status": each["status"],
                    "result": res_url,
                }
            )

    context = {"result": data, "Success": True}

    return JsonResponse(context, safe=False)


def pipe_create(request):
    """ Creates pipeline for the given resource_id and executes a transformation on it."""
    if request.method == 'POST':
        post_data = json.loads(request.body.decode('utf-8'))
        pipeline_name = post_data.get('pipeline_name', None)
        p = Pipeline(status="Created", pipeline_name=pipeline_name)
        p.save()

        p_id = p.pk

        # logger = log_utils.set_log_file(p_id, pipeline_name)
        # logger.info("INFO: New pipeline is created with id - ",p_id)

        res_id = post_data.get('res_id', None)
        dataset_id = post_data.get('dataset_id', None)
        db_action = post_data.get('db_action', None)

        # if the task is to create new res - new_res_id is returned at next step, now it's None.
        if db_action == "create":
            new_res_id = None
        else:
            # if the db action is to 'update' resultant_id is same as res_id
            new_res_id = res_id
            p.resultant_res_id = res_id
        logger = log_utils.set_log_file(p_id, pipeline_name)
        # transformers_list = post_data.get("transformers_list", None)
        # transformers_list = [i for i in transformers_list if i]
        # for _, each in enumerate(transformers_list):
        #     task_name = each.get("name", None)
        #     task_order_no = each.get("order_no", None)
        #     task_context = each.get("context", None)
        #     p.task_set.create(
        #         task_name=task_name,
        #         status="Created",
        #         order_no=task_order_no,
        #         context=task_context,
        #     )
        # logger.info(
        #     f"INFO:Received request to create pipeline {pipeline_name} with these tasks"
        #     f"{transformers_list}"
        # )
        p.dataset_id = dataset_id
        p.resource_identifier = res_id
        p.db_action = db_action
        p.save()

        pipeline_creator_bg.create_pipeline(post_data, p_id)
        context = {"result": {
            "p_id": p_id,
            "new_res_id": new_res_id
        },
            "Success": True}
        return JsonResponse(context, safe=False)


def pipe_update(request):
    """ Hit this api to add more tasks to the existing pipeline """
    if request.method == 'POST':
        post_data = json.loads(request.body.decode('utf-8'))
        p_id = post_data.get('p_id', None)
        pipeline_object = Pipeline.objects.get(pk=p_id)
        pipeline_name = getattr(pipeline_object, "pipeline_name")
        logger = log_utils.get_logger_for_existing_file(p_id)
        transformers_list = post_data.get("transformers_list", None)
        transformers_list = [i for i in transformers_list if i]
        for _, each in enumerate(transformers_list):
            task_name = each.get("name", None)
            task_order_no = each.get("order_no", None)
            task_context = each.get("context", None)
            pipeline_object.task_set.create(
                task_name=task_name,
                status="Created",
                order_no=task_order_no,
                context=task_context,
            )
        logger.info(f"INFO: Received request to update the pipeline - {pipeline_name} "
                    f"with the task - {transformers_list}")
        pipeline_creator_bg.create_pipeline(post_data, p_id)
        context = {"result": {
            "p_id": p_id,
        },
            "Success": True}
        return JsonResponse(context, safe=False)


def read_data(data_url):
    all_data = pd.read_csv(data_url)
    all_data.fillna(value="", inplace=True)

    return all_data


def res_transform(request):
    """Triggers pipeline_creator_bg background task."""
    if request.method == "POST":
        post_data = json.loads(request.body.decode("utf-8"))
        pipeline_name = post_data.get("pipeline_name", None)
        dataset_id = post_data.get("dataset_id", None)
        db_action = post_data.get("db_action", None)
        p = Pipeline(
            status="Requested",
            pipeline_name=pipeline_name,
            dataset_id=dataset_id,
            db_action=db_action,
        )
        p.save()

        p_id = p.pk
        logger = log_utils.set_log_file(p_id, pipeline_name)
        transformers_list = post_data.get("transformers_list", None)
        transformers_list = [i for i in transformers_list if i]
        for _, each in enumerate(transformers_list):
            task_name = each.get("name", None)
            task_order_no = each.get("order_no", None)
            task_context = each.get("context", None)
            p.task_set.create(
                task_name=task_name,
                status="Created",
                order_no=task_order_no,
                context=task_context,
            )
        logger.info(
            f"INFO:Received request to create pipeline {pipeline_name} with these tasks"
            f"{transformers_list}"
        )
        pipeline_creator_bg.create_pipeline(post_data, p_id)
        completed_tasks_qs = CompletedTask.objects.all()
        print(completed_tasks_qs)
        context = {"result": pipeline_name, "Success": True}
        return JsonResponse(context, safe=False)


def api_res_transform(request):
    # save the pipeline data against api resource id
    if request.method == "POST":
        post_data = json.loads(request.body.decode("utf-8"))
        api_source_id = post_data.get("api_source_id", None)
        dataset_id = post_data.get("dataset_id", None)
        transformers_list = post_data.get("transformers_list", None)
        transformers_list = [i for i in transformers_list if i]
        pipeline_name = post_data.get("pipeline_name", None)
        p = Pipeline(
            status="Created",
            pipeline_name=pipeline_name,
            resource_identifier=str(api_source_id),
            dataset_id=dataset_id,
        )
        print(api_source_id)
        print("***", p.resource_identifier)
        p.save()
        p_id = p.pk

        for _, each in enumerate(transformers_list):
            task_name = each.get("name", None)
            task_order_no = each.get("order_no", None)
            task_context = each.get("context", None)
            p.task_set.create(
                task_name=task_name,
                status="Created",
                order_no=task_order_no,
                context=task_context,
            )
        p.save()

        context = {"result": p_id, "Success": True}
        return JsonResponse(context, safe=False)

def delete_api_res_transform(request):
    if request.method == "POST":
        post_data = json.loads(request.body.decode("utf-8"))
        pipeline_id = post_data.get("pipeline_id", None)
        Pipeline.objects.filter(pipeline_id=pipeline_id).delete()
        context = {"Success": True}
        return JsonResponse(context, safe=False)
    
def clone_pipe(request):
    if request.method == "POST":
        post_data = json.loads(request.body.decode("utf-8"))
        #print ('---------------------clonedata', post_data)
        pipeline_id = post_data.get("pipeline_id", None)
        dataset_id = post_data.get("dataset_id", None)
        resource_id = post_data.get("resource_id", None)
        resultant_res_id = post_data.get("resultant_res_id", None)
        
        pipe_clone = Pipeline.objects.get(pipeline_id=pipeline_id)
        pipe_clone.pk = None
        setattr(pipe_clone, 'dataset_id', dataset_id)
        setattr(pipe_clone, 'resource_identifier', resource_id)
        setattr(pipe_clone, 'resultant_res_id', resultant_res_id)
        pipe_clone.save()
        
        
        tasks = Task.objects.filter(Pipeline_id=pipeline_id)
        for task in tasks:
            task_clone = Task.objects.get(task_id=task.task_id)
            task_clone.pk = None
        
            setattr(task_clone, 'Pipeline_id', pipe_clone)
            task_clone.save()
        
        
                
        context = {"Success": True}
        return JsonResponse(context, safe=False)


def api_res_run_transform(request):
    if request.method == "POST":
        post_data = json.loads(request.body.decode("utf-8"))
        api_source_id = str(post_data.get("api_source_id", None))
        api_data_params = post_data.get("api_data_params", {})
        print("0", post_data)
        try:
            pipeline_object = list(
                Pipeline.objects.filter(resource_identifier=api_source_id)
            )[-1]
            print("got an obj")
            p_id = getattr(pipeline_object, "pipeline_id")
        except Exception as e:
            print(str(e))
            print("3---")
            p_id = None

        try:
            resp_data, response_type = api_res_run_transform_task(
                p_id,
                api_source_id,
                api_data_params
            )
        except Exception as e:
            print(str(e))

        #print ('*****', resp_data)

        if response_type.lower() == "csv":
            resp_data = resp_data.to_dict('records') #string(index=False)

        #print ('------111----', resp_data)

        context = {"Success": True, "data": resp_data, "response_type": response_type}
        return JsonResponse(context, safe=False)


def api_source_query(request):
    if request.method == "POST":
        post_data = json.loads(request.body.decode("utf-8"))
        api_source_id = str(post_data.get("api_source_id", None))
        request_id = post_data.get("request_id", None)
        request_columns = post_data.get("request_columns", "")
        remove_nodes = post_data.get("remove_nodes", [])
        request_rows = post_data.get("request_rows", "")
        target_format = post_data.get("target_format", "")
        print("0", post_data)
        try:
            pipeline_object = list(
                Pipeline.objects.filter(resource_identifier=api_source_id)
            )[-1]
            print("got an obj")
            p_id = getattr(pipeline_object, "pipeline_id")
        except Exception as e:
            print(str(e))
            print("3---")
            p_id = None
        print("abcd;")
        api_resource_query_task(
            p_id,
            api_source_id,
            request_id,
            request_columns,
            remove_nodes,
            request_rows,
            target_format,
        )
        print("def")

        context = {"result": p_id, "Success": True}
        return JsonResponse(context, safe=False)


def custom_data_viewer(request):
    if request.method == "POST":
        post_data = json.loads(request.body.decode("utf-8"))
        print("data received..", post_data)
        res_id = post_data.get("res_id")
        columns = post_data.get("columns")
        num_rows = post_data.get("rows")
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
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = "attachment; filename=export.csv"
        final_df.to_csv(path_or_buf=response, index=False)  # with other applicable parameters
        return response
