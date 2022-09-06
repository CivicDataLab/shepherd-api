import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pdfkit
from json2xml import json2xml
from prefect import task, flow, get_run_logger
from utils import upload_resource, create_resource

def skip_column(context, pipeline, task_obj):
    column = context['columns']
    try:
        pipeline.data = pipeline.data.drop(column, axis=1)
    except Exception as e:
        send_error_to_prefect_cloud(e)
    set_task_model_values(task_obj, pipeline)


@task
def merge_columns(context, pipeline, task_obj):
    print(context)
    print("inside merge cols...")
    column1, column2, output_column = context['column1'], context['column2'], context['output_column']
    separator = context['separator']
    print(pipeline.data[column1])
    try:
        print("inside try of merge_col")
        pipeline.data[output_column] = pipeline.data[column1].astype(str) + separator + pipeline.data[column2] \
            .astype(str)
        pipeline.data = pipeline.data.drop([column1, column2], axis=1)
    except Exception as e:
        send_error_to_prefect_cloud(e)
    set_task_model_values(task_obj, pipeline)


@task
def anonymize(context, pipeline, task_obj):
    # TODO - decide on the context contents
    to_replace = context['to_replace']
    replace_val = context['replace_val']
    col = context['column']
    try:
        df_updated = pipeline.data[col].str.replace(re.compile(to_replace, re.IGNORECASE), replace_val)
        df_updated = df_updated.to_frame()
        pipeline.data[col] = df_updated[col]
    except Exception as e:
        send_error_to_prefect_cloud(e)
    set_task_model_values(task_obj, pipeline)


@task
def change_format(context, pipeline, task_obj):
    # TODO - decide on the context contents
    file_format = context['format']
    if file_format == "xml":
        data_string = pipeline.data.to_json(orient='records')
        json_data = json.loads(data_string)
        xml_data = json2xml.Json2xml(json_data).to_xml()
        print(xml_data)
        with open('xml_data.xml', 'w') as f:
            f.write(xml_data)
    elif file_format == "pdf":
        pipeline.data.to_html("data.html")
        pdfkit.from_file("data.html", "pdf_data.pdf")
        os.remove('data.html')
    elif file_format == "json":
        data_string = pipeline.data.to_json(orient='records')
        with open("json_data.json", "w") as f:
            f.write(data_string)

    set_task_model_values(task_obj, pipeline)


@task
def aggregate(context, pipeline, task_obj):
    index = context['index']
    columns = context['columns']
    values = context['values']
    columns = columns.split(",")
    values = values.split(",")
    pipeline.data = pd.pivot(pipeline.data, index=index, columns=columns, values=values)
    set_task_model_values(task_obj, pipeline)


@flow
def pipeline_executor(pipeline):
    print("setting ", pipeline.model.pipeline_name, " status to In Progress")
    pipeline.model.status = "In Progress"
    print(pipeline.model.status)
    pipeline.model.save()
    tasks_objects = pipeline._commands
    func_names = get_task_names(tasks_objects)
    contexts = get_task_contexts(tasks_objects)
    try:
        for i in range(len(func_names)):
            globals()[func_names[i]](contexts[i], pipeline, tasks_objects[i])
    except Exception as e:
        raise e
    pipeline.model.status = "Done"
    print("Data after pipeline execution\n", pipeline.data)
    pipeline.model.save()
    return


def get_task_names(task_obj_list):
    task_names = []
    for obj in task_obj_list:
        task_names.append(obj.task_name)
    return task_names


def get_task_contexts(task_obj_list):
    contexts = []
    for obj in task_obj_list:
        context = json.loads(obj.context.replace('\'', '"'))
        contexts.append(context)
    return contexts


def set_task_model_values(task, pipeline):
    task.output_id = '1'
    # create_resource(
    #     {'package_id': pipeline.model.output_id, 'resource_name': task.task_name, 'data': pipeline.data})
    print({'package_id': task.output_id, 'resource_name': task.task_name, 'data': pipeline.data})
    task.status = "Done"
    task.save()


def send_error_to_prefect_cloud(e:Exception):
    prefect_logger = get_run_logger()
    prefect_logger.error(str(e))