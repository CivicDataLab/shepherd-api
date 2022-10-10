import os
import re

import pandas as pd
import pdfkit
from json2xml import json2xml
from pandas.io.json import build_table_schema
from prefect import task, flow
from task_utils import *


@task
def skip_column(context, pipeline, task_obj):
    column = context['columns']
    col = column
    if not isinstance(column, list):
        column = list()
        column.append(col)
    try:
        pipeline.data = pipeline.data.drop(column, axis=1)
        for col in column:
            for sc in pipeline.schema:
                if sc['key'] == col:
                    sc['key'] = ""
                    sc['format'] = ""
                    sc['description'] = ""
        set_task_model_values(task_obj, pipeline)
    except Exception as e:
        send_error_to_prefect_cloud(e)
        task_obj.status = "Failed"
        task_obj.save()


@task
def merge_columns(context, pipeline, task_obj):
    column1, column2, output_column = context['column1'], context['column2'], context['output_column']
    separator = context['separator']

    try:
        pipeline.data[output_column] = pipeline.data[column1].astype(str) + separator + pipeline.data[column2] \
            .astype(str)
        pipeline.data = pipeline.data.drop([column1, column2], axis=1)

        """ setting up the schema after task"""
        data_schema = pipeline.data.convert_dtypes(infer_objects=True, convert_string=True,
                                                   convert_integer=True, convert_boolean=True, convert_floating=True)
        names_types_dict = data_schema.dtypes.astype(str).to_dict()
        new_col_format = names_types_dict[output_column]
        for sc in pipeline.schema:
            if sc['key'] == column1:
                sc['key'] = ""
                sc['format'] = ""
                sc['description'] = ""
            if sc['key'] == column2:
                sc['key'] = ""
                sc['format'] = ""
                sc['description'] = ""
        pipeline.schema.append({
            "key": output_column, "format": new_col_format,
            "description": "Result of merging columns " + column1 + " & " + column2 + " by pipeline - "
                           + pipeline.model.pipeline_name
        })
        set_task_model_values(task_obj, pipeline)
    except Exception as e:
        send_error_to_prefect_cloud(e)
        task_obj.status = "Failed"
        task_obj.save()


@task
def anonymize(context, pipeline, task_obj):
    # TODO - decide on the context contents
    to_replace = context['to_replace']
    replace_val = context['replace_val']
    col = context['column']
    try:
        df_updated = pipeline.data[col].astype(str).str.replace(re.compile(to_replace, re.IGNORECASE), replace_val)
        df_updated = df_updated.to_frame()
        pipeline.data[col] = df_updated[col]
        data_schema = pipeline.data.convert_dtypes(infer_objects=True, convert_string=True,
                                                   convert_integer=True, convert_boolean=True, convert_floating=True)
        names_types_dict = data_schema.dtypes.astype(str).to_dict()

        for sc in pipeline.schema:
            if sc['key'] == col:
                sc['format'] = names_types_dict[col]
        set_task_model_values(task_obj, pipeline)
    except Exception as e:
        send_error_to_prefect_cloud(e)
        task_obj.status = "Failed"
        task_obj.save()


@task
def change_format(context, pipeline, task_obj):
    # TODO - decide on the context contents
    file_format = context['format']
    result_file_name = pipeline.model.pipeline_name
    if file_format == "xml" or file_format =="XML":
        try:
            data_string = pipeline.data.to_json(orient='records')
            json_data = json.loads(data_string)
            xml_data = json2xml.Json2xml(json_data).to_xml()
            print(xml_data)
            with open(result_file_name + '.xml', 'w') as f:
                f.write(xml_data)
            set_task_model_values(task_obj, pipeline)
        except Exception as e:
            send_error_to_prefect_cloud(e)
            task_obj.status = "Failed"
            task_obj.save()
    elif file_format == "pdf" or file_format == "PDF":
        try:
            pipeline.data.to_html("data.html")
            pdfkit.from_file("data.html", result_file_name + ".pdf")
            os.remove('data.html')
            set_task_model_values(task_obj, pipeline)
        except Exception as e:
            send_error_to_prefect_cloud(e)
            task_obj.status = "Failed"
            task_obj.save()
    elif file_format == "json" or file_format == "JSON":
        try:
            data_string = pipeline.data.to_json(orient='records')
            with open(result_file_name + ".json", "w") as f:
                f.write(data_string)
            set_task_model_values(task_obj, pipeline)
        except Exception as e:
            send_error_to_prefect_cloud(e)
            task_obj.status = "Failed"
            task_obj.save()


@task
def aggregate(context, pipeline, task_obj):
    print("inside aggregate")
    index = context['index']
    columns = context['columns']
    values = context['values']
    columns = columns.split(",")
    values = values.split(",")
    try:
        pipeline.data = pd.pivot(pipeline.data, index=index, columns=columns, values=values)
        inferred_schema = build_table_schema(pipeline.data)
        fields = inferred_schema['fields']
        new_schema = []
        for field in fields:
            key = field['name']
            description = ""
            format = field['type']
            for sc in pipeline.schema:
                if sc['key'] == key or sc['key'] == key[0]:
                    description = sc['description']
            if isinstance(key, tuple):
                key = " ".join(map(str, key))
            new_schema.append({"key": key, "format": format, "description": description})
        pipeline.schema = new_schema
        set_task_model_values(task_obj, pipeline)
    except Exception as e:
        send_error_to_prefect_cloud(e)
        task_obj.status = "Failed"
        task_obj.save()


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
    # if any of the tasks is failed, set pipeline status as - Failed
    for each_task in tasks_objects:
        if each_task.status == "Failed":
            pipeline.model.status = "Failed"
            pipeline.model.save()
            break
    # if none of the tasks failed, set pipeline status as - Done
    if pipeline.model.status != "Failed":
        pipeline.model.status = "Done"
        pipeline.model.save()
    pipeline.model.output_id = str(pipeline.model.pipeline_id) + "_" + pipeline.model.status
    print("Data after pipeline execution\n", pipeline.data)
    pipeline.model.save()
    return
