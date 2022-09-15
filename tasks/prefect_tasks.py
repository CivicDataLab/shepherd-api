import os
import re

import pandas as pd
import pdfkit
from json2xml import json2xml
from prefect import task, flow
from task_utils import *


@task
def skip_column(context, pipeline, task_obj):
    column = context['columns']

    data_schema = pipeline.data.convert_dtypes(infer_objects=True, convert_string=True,
                                        convert_integer=True, convert_boolean=True, convert_floating=True)

    names_types_dict = pipeline.data.dtypes.astype(str).to_dict()

    key_entry = column
    print("donw here..")
    format_entry = list()
    print(column)
    if isinstance(column, list):
        for col in column:
            format_entry.append(names_types_dict[col])
    else:
        format_entry = names_types_dict[column]
    description_entry = "performed "+ task_obj.task_name + " under "+ pipeline.model.pipeline_name

    pipeline.schema["key"].append(key_entry)
    pipeline.schema["format"].append(format_entry)
    pipeline.schema["description"].append(description_entry)

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

    names_types_dict = pipeline.data.dtypes.astype(str).to_dict()
    print("here I am...", names_types_dict)
    key_entry1 = column1
    key_entry2 = column2
    format_entry1 = names_types_dict[column1]
    format_entry2 = names_types_dict[column2]
    print("format entries...", format_entry1, format_entry2)
    description_entry = "performed " + task_obj.task_name + " under " + pipeline.model.pipeline_name

    pipeline.schema["key"].append(key_entry1)
    pipeline.schema["key"].append(key_entry2)
    pipeline.schema["format"].append(format_entry1)
    pipeline.schema["format"].append(format_entry2)
    pipeline.schema["description"].append(description_entry)

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

    names_types_dict = pipeline.data.dtypes.astype(str).to_dict()
    key_entry = col
    format_entry = names_types_dict[col]
    description_entry = "performed " + task_obj.task_name + " under " + pipeline.model.pipeline_name
    schema_dict = {"key": key_entry, "format": format_entry, "description": description_entry}
    pipeline.schema.append(schema_dict)
    # pipeline.schema["key"].append(key_entry)
    # pipeline.schema["format"].append(format_entry)
    # pipeline.schema["description"].append(description_entry)
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
    result_file_name = pipeline.model.pipeline_name + "-" + task_obj.task_name
    if file_format == "xml":
        data_string = pipeline.data.to_json(orient='records')
        json_data = json.loads(data_string)
        xml_data = json2xml.Json2xml(json_data).to_xml()
        print(xml_data)
        with open(result_file_name +'.xml', 'w') as f:
            f.write(xml_data)
    elif file_format == "pdf":
        pipeline.data.to_html("data.html")
        pdfkit.from_file("data.html", result_file_name + ".pdf")
        os.remove('data.html')
    elif file_format == "json":
        data_string = pipeline.data.to_json(orient='records')
        with open(result_file_name + ".json", "w") as f:
            f.write(data_string)

    set_task_model_values(task_obj, pipeline)


@task
def aggregate(context, pipeline, task_obj):
    index = context['index']
    columns = context['columns']
    values = context['values']
    columns = columns.split(",")
    values = values.split(",")

    key_entry = columns
    format_entry = pipeline.data.dtypes[columns]
    description_entry = "performed " + task_obj.task_name + " under " + pipeline.model.pipeline_name

    pipeline.schema["key"].append(key_entry)
    pipeline.schema["format"].append(format_entry)
    pipeline.schema["description"].append(description_entry)

    pipeline.data = pd.pivot(pipeline.data, index=index, columns=columns, values=values)
    set_task_model_values(task_obj, pipeline)


@task
def query_data_resource(context, pipeline, task_obj):
    columns = context['columns']
    num_rows = int(context["rows"])

    if len(columns) == 0:
        column_selected_df = pipeline.data
    else:
        column_selected_df = pipeline.data.loc[:, pipeline.data.columns.isin(columns)]
    # if row length is not specified return all rows
    if num_rows == "" or int(num_rows) > len(column_selected_df):
        final_df = column_selected_df
    else:
        num_rows_int = int(num_rows)
        final_df = column_selected_df.iloc[:num_rows_int]
    pipeline.data = final_df

    key_entry = columns
    format_entry = pipeline.data.dtypes[columns]
    description_entry = "performed " + task_obj.task_name + " under " + pipeline.model.pipeline_name

    pipeline.schema["key"].append(key_entry)
    pipeline.schema["format"].append(format_entry)
    pipeline.schema["description"].append(description_entry)

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
    pipeline.model.output_id = str(pipeline.model.pipeline_id) + "_" + pipeline.model.status
    print("Data after pipeline execution\n", pipeline.data)
    pipeline.model.save()
    return
