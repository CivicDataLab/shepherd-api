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
    col = column
    if not isinstance(column, list):
        column = list()
        column.append(col)

    # data_schema = pipeline.data.convert_dtypes(infer_objects=True, convert_string=True,
    #                                            convert_integer=True, convert_boolean=True, convert_floating=True)
    #
    # names_types_dict = data_schema.dtypes.astype(str).to_dict()
    # key_entry = ""
    # format_entry = ""
    # for col in column:
    #     key_entry = key_entry + col + ","
    #     format_entry = format_entry + names_types_dict[col] + ","
    # key_entry = key_entry.rstrip()
    # format_entry = format_entry.rstrip()
    # description_entry = "performed " + task_obj.task_name + " by " + pipeline.model.pipeline_name
    # schema = populate_task_schema(key_entry, format_entry, description_entry)
    # pipeline.schema.append(schema)
    try:
        pipeline.data = pipeline.data.drop(column, axis=1)
        for col in column:
            for sc in pipeline.schema:
                if sc['key'] == col:
                    sc['key'] = ""
                    sc['format'] = ""
                    sc['description'] = ""
    except Exception as e:
        send_error_to_prefect_cloud(e)

    set_task_model_values(task_obj, pipeline)


@task
def merge_columns(context, pipeline, task_obj):
    column1, column2, output_column = context['column1'], context['column2'], context['output_column']
    separator = context['separator']

    # key_entry_list = str(column1) + ", " + str(column2)
    # format_1 = names_types_dict[column1]
    # format_2 = names_types_dict[column2]
    # format_entry = str(format_1) + ", " + str(format_2)
    # description_entry = "performed " + task_obj.task_name + " by " + pipeline.model.pipeline_name
    # pipeline.schema.append(populate_task_schema(key_entry_list, format_entry, description_entry))

    try:
        print("inside try of merge_col")
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
                print(sc ," got resetted..$$$$$")
            if sc['key'] == column2:
                sc['key'] = ""
                sc['format'] = ""
                sc['description'] = ""
                print(sc ," got resetted..$$$$$")
        pipeline.schema.append({
            "key": output_column, "format": new_col_format,
            "description": "Result of merging columns " + column1 + " & " + column2 + " by pipeline - "
                           + pipeline.model.pipeline_name
        })

    except Exception as e:
        send_error_to_prefect_cloud(e)


    set_task_model_values(task_obj, pipeline)


@task
def anonymize(context, pipeline, task_obj):
    # TODO - decide on the context contents
    to_replace = context['to_replace']
    replace_val = context['replace_val']
    col = context['column']

    # key_entry = col
    # format_entry = names_types_dict[col]
    # description_entry = "performed " + task_obj.task_name + " by " + pipeline.model.pipeline_name
    # pipeline.schema.append(populate_task_schema(key_entry, format_entry, description_entry))

    try:
        df_updated = pipeline.data[col].str.replace(re.compile(to_replace, re.IGNORECASE), replace_val)
        df_updated = df_updated.to_frame()
        pipeline.data[col] = df_updated[col]
    except Exception as e:
        send_error_to_prefect_cloud(e)

    data_schema = pipeline.data.convert_dtypes(infer_objects=True, convert_string=True,
                                               convert_integer=True, convert_boolean=True, convert_floating=True)
    names_types_dict = data_schema.dtypes.astype(str).to_dict()

    for sc in pipeline.schema:
        if sc['key'] == col:
            sc['format'] = names_types_dict[col]
    set_task_model_values(task_obj, pipeline)


@task
def change_format(context, pipeline, task_obj):
    # TODO - decide on the context contents
    file_format = context['format']
    result_file_name = pipeline.model.pipeline_name
    print("FORMAAAAAAT", file_format)
    if file_format == "xml" or file_format =="XML":
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
    index = context['index']
    columns = context['columns']
    values = context['values']
    columns = columns.split(",")
    values = values.split(",")

    data_schema = pipeline.data.convert_dtypes(infer_objects=True, convert_string=True,
                                               convert_integer=True, convert_boolean=True, convert_floating=True)
    names_types_dict = data_schema.dtypes.astype(str).to_dict()
    try:
        pipeline.data = pd.pivot(pipeline.data, index=index, columns=columns, values=values)
        for sc in pipeline.schema:
            if sc['key'] == index:
                sc['key'] = ""
                sc['format'] = ""
                sc['description'] = ""
    except:
        pass
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
