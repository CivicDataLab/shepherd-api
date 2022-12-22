"""
Each task is nothing but a publisher. These tasks go on publishing the pipeline tasks one by
one in the given order and receive the data back from the workers.
Python specific operations - like updating the pipeline data, schema etc. are the responsibilities of prefect tasks.
Actual tasks can be found under tasks/scripts which can be implemented in any language of choice.
"""

import os
import re

import pandas as pd
import pdfkit
from json2xml import json2xml
from pandas.io.json import build_table_schema
from prefect import task, flow
from task_utils import *
from task_utils import TasksRpcClient
from io import StringIO


@task
def skip_column(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data)
    if not exception_flag:
        df = pd.read_csv(StringIO(data), sep=',')
        df = remove_unnamed_col(df)
        pipeline.data = df
        print("data%%%%", pipeline.data)
        set_task_model_values(task_obj, pipeline)


@task
def merge_columns(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data)
    if not exception_flag:
        df = pd.read_csv(StringIO(data), sep=',')
        df = remove_unnamed_col(df)
        pipeline.data = df
        print("data%%%%", pipeline.data)
        set_task_model_values(task_obj, pipeline)


@task
def anonymize(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data)
    if not exception_flag:
        df = pd.read_csv(StringIO(data), sep=',')
        df = remove_unnamed_col(df)
        pipeline.data = df
        print("data%%%%", pipeline.data)
        set_task_model_values(task_obj, pipeline)


@task
def change_format(context, pipeline, task_obj):
    # TODO - decide on the context contents
    file_format = context['format']
    result_file_name = pipeline.model.pipeline_name
    print("FORMAAAAAAT", file_format)
    if file_format == "xml" or file_format == "XML":
        data_string = pipeline.data.to_json(orient='records')
        json_data = json.loads(data_string)
        xml_data = json2xml.Json2xml(json_data).to_xml()
        print(xml_data)
        with open(result_file_name + '.xml', 'w') as f:
            f.write(xml_data)
    elif file_format == "pdf" or file_format == "PDF":
        pipeline.data.to_html("data.html")
        pdfkit.from_file("data.html", result_file_name + ".pdf")
        os.remove('data.html')
    elif file_format == "json" or file_format == "JSON":
        data_string = pipeline.data.to_json(orient='records')
        with open(result_file_name + ".json", "w") as f:
            f.write(data_string)
    set_task_model_values(task_obj, pipeline)


@task
def aggregate(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data)
    if not exception_flag:
        df = pd.read_csv(StringIO(data), sep=',')
        df = remove_unnamed_col(df)
        pipeline.data = df
        print("data%%%%", pipeline.data)
        set_task_model_values(task_obj, pipeline)

@task
def query_data_resource(context, pipeline, task_obj):
    columns = context['columns']
    num_rows = context["rows"]
    columns = columns.split(",")
    if len(columns) == 1 and len(columns[0]) == 0:
        column_selected_df = pipeline.data
    else:
        column_selected_df = pipeline.data.loc[:, pipeline.data.columns.isin(columns)]
        for sc in pipeline.schema:
            if sc["key"] not in columns:
                sc["key"] = ""
                sc["format"] = ""
                sc["description"] = ""
    # if row length is not specified return all rows
    if num_rows == "" or int(num_rows) > len(column_selected_df):
        final_df = column_selected_df
    else:
        num_rows_int = int(num_rows)
        final_df = column_selected_df.iloc[:num_rows_int]
    pipeline.data = final_df

    data_schema = pipeline.data.convert_dtypes(infer_objects=True, convert_string=True,
                                               convert_integer=True, convert_boolean=True, convert_floating=True)
    names_types_dict = data_schema.dtypes.astype(str).to_dict()

    set_task_model_values(task_obj, pipeline)

@task
def fill_missing_fields(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data)
    if not exception_flag:
        print(data, "???")



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
    for task in tasks_objects:
        if task.status == "Failed":
            pipeline.model.status = "Failed"
            pipeline.model.save()
            break
    if pipeline.model.status != "Failed":
        pipeline.model.status = "Done"
        pipeline.model.save()
    pipeline.model.output_id = str(pipeline.model.pipeline_id) + "_" + pipeline.model.status
    print("Data after pipeline execution\n", pipeline.data)
    return
