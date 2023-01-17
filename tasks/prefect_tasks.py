import os
import random
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
        pipeline.logger.info(f"INFO: skip_column task is done")
        set_task_model_values(task_obj, pipeline)
    except Exception as e:
        send_error_to_prefect_cloud(e)
        pipeline.logger.error(f"ERROR: skip_column failed with an error - {str(e)}. Setting "
                             f"pipeline status to failed")
        pipeline.model.err_msg = (f"ERROR: skip_column failed with an error - {str(e)}. Setting "
                             f"pipeline status to failed")
        pipeline.model.save()
        task_obj.status = "Failed"

        task_obj.save()


@task
def merge_columns(context, pipeline, task_obj):
    column1, column2, output_column = context['column1'], context['column2'], context['output_column']
    retain_cols = False
    separator = context['separator']
    try:
        retain_cols = context['retain_cols']
    except:
        pass
    try:
        pipeline.data[output_column] = pipeline.data[column1].astype(str) + separator + pipeline.data[column2] \
            .astype(str)
        if not retain_cols:
            pipeline.data = pipeline.data.drop([column1, column2], axis=1)

        """ setting up the schema after task"""
        data_schema = pipeline.data.convert_dtypes(infer_objects=True, convert_string=True,
                                                   convert_integer=True, convert_boolean=True, convert_floating=True)
        names_types_dict = data_schema.dtypes.astype(str).to_dict()
        new_col_format = names_types_dict[output_column]
        if not retain_cols:
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
            "key": output_column, "format": new_col_format,"parent": "", "array_field":"", "path":"", "parent_path":"",
            "description": "Result of merging columns " + column1 + " & " + column2 + " by pipeline - "
                           + pipeline.model.pipeline_name
        })
        pipeline.logger.info(f"INFO: task - merge_columns is done.")
        set_task_model_values(task_obj, pipeline)
    except Exception as e:
        pipeline.logger.error(f"ERROR: merge_columns failed with an error - {str(e)}. Setting "
                              f"pipeline status to failed")
        pipeline.model.err_msg = (f"ERROR: merge_columns failed with an error - {str(e)}. Setting "
                              f"pipeline status to failed")
        pipeline.model.save()
        send_error_to_prefect_cloud(e)
        task_obj.status = "Failed"
        task_obj.save()


@task
def anonymize(context, pipeline, task_obj):
    # TODO - decide on the context contents
    option = context['option']
    special_char = context['special_char']
    col = context['column']
    try:
        df_col_values = pipeline.data[col].values.tolist()
        new_vals = []
        for val in df_col_values:
            val = str(val)
            if option == "replace_all":
                if special_char == "random":
                    replace_val = "".join(random.choices("!@#$%^&*()<>?{}[]~`", k=len(val)))
                    new_vals.append(replace_val)
                else:
                    replace_val = special_char * len(val)
                    new_vals.append(replace_val)
            elif option == "replace_nth":
                n = context.get('n')
                n = int(n)  #- 1
                if special_char == "random":
                    replacement = "".join(random.choices("!@#$%^&*()<>?{}[]~`", k=1))
                                        
                    for i in range((n-1), len(val)+(n-1), n):
                        val = val[ : i] + replacement + val[i + 1: ] 
                    # for i in range(0, len(val), int(n)):
                    #     val = val[ : i] + replacement + val[i + 1: ] 
                    # replace_val = val[0:int(n)] + replacement + val[int(n) + 1:]
                    new_vals.append(val)
                else:
                    for i in range((n-1), len(val)+(n-1), n):
                        val = val[ : i] + special_char + val[i + 1: ]                     
                    # for i in range(0, len(val), int(n)):
                    #     val = val[ : i] + special_char + val[i + 1: ] 
                    new_vals.append(val)
            elif option == "retain_first_n":
                n = context.get('n')
                if special_char == "random":
                    replacement = "".join(random.choices("!@#$%^&*()<>?{}[]~`", k=(len(val) - int(n))))
                    replace_val = val[:int(n)] + replacement
                    new_vals.append(replace_val)
                else:
                    replace_val = val[:int(n)] + (special_char * (len(val) - int(n)))
                    new_vals.append(replace_val)
        pipeline.data[col] = new_vals

        data_schema = pipeline.data.convert_dtypes(infer_objects=True, convert_string=True,
                                                   convert_integer=True, convert_boolean=True, convert_floating=True)
        names_types_dict = data_schema.dtypes.astype(str).to_dict()

        for sc in pipeline.schema:
            if sc['key'] == col:
                sc['format'] = names_types_dict[col]
        pipeline.logger.info(f"INFO: task - anonymize task is done")
        set_task_model_values(task_obj, pipeline)
    except Exception as e:
        pipeline.logger.error(f"ERROR: task - anonymize failed with an error - {str(e)}. Setting "
                              f"pipeline status to failed")
        pipeline.model.err_msg = (f"ERROR: task - anonymize failed with an error - {str(e)}. Setting "
                              f"pipeline status to failed")
        pipeline.model.save()
        send_error_to_prefect_cloud(e)
        task_obj.status = "Failed"
        task_obj.save()


@task
def change_format_to_pdf(context, pipeline, task_obj):
    file_format = context['format']
    result_file_name = pipeline.model.pipeline_name
    dir = "format_changed_files/"
    if file_format == "xml" or file_format =="XML":
        try:
            data_string = pipeline.data.to_json(orient='records')
            json_data = json.loads(data_string)
            xml_data = json2xml.Json2xml(json_data).to_xml()
            with open(dir+result_file_name + '.xml', 'w') as f:
                f.write(xml_data)
            pipeline.logger.info(f"INFO: Resource format changed to xml")
            set_task_model_values(task_obj, pipeline)
        except Exception as e:
            pipeline.logger.error(f"ERROR: task - change_format failed with an error - {str(e)}. Setting "
                                  f"pipeline status to failed")
            pipeline.model.err_msg = (f"ERROR: task - change_format failed with an error - {str(e)}. Setting "
                                  f"pipeline status to failed")
            pipeline.model.save()
            send_error_to_prefect_cloud(e)
            task_obj.status = "Failed"
            task_obj.save()
    elif file_format == "pdf" or file_format == "PDF":
        try:
            pipeline.data.to_html("data.html", index=False)
            pdfkit.from_file("data.html", dir+result_file_name + ".pdf")
            os.remove('data.html')
            pipeline.logger.info(f"INFO: Resource format changed to pdf")
            set_task_model_values(task_obj, pipeline)
        except Exception as e:
            pipeline.logger.error(f"ERROR: task - change_format failed with an error - {str(e)}. Setting "
                                  f"pipeline status to failed")
            pipeline.model.err_msg = (f"ERROR: task - change_format failed with an error - {str(e)}. Setting "
                                  f"pipeline status to failed")
            pipeline.model.save()
            send_error_to_prefect_cloud(e)
            task_obj.status = "Failed"
            task_obj.save()
    elif file_format == "json" or file_format == "JSON":
        try:
            data_string = pipeline.data.to_json(orient='records')
            with open(dir + result_file_name + ".json", "w") as f:
                f.write(data_string)
            pipeline.logger.info(f"INFO: Resource format changed to json")
            set_task_model_values(task_obj, pipeline)
        except Exception as e:
            pipeline.logger.error(f"ERROR: task - change_format failed with an error - {str(e)}. Setting "
                                  f"pipeline status to failed")
            pipeline.model.err_msg = (f"ERROR: task - change_format failed with an error - {str(e)}. Setting "
                                  f"pipeline status to failed")
            pipeline.model.save()
            send_error_to_prefect_cloud(e)
            task_obj.status = "Failed"
            task_obj.save()


@task
def aggregate(context, pipeline, task_obj):
    print("inside aggregate")
    index = context['index']
    columns = context['columns']
    values = context['values']
    index = index.split(",")
    columns = columns.split(",")
    values = values.split(",")
    none_list = [None]
    for col in columns:
        none_list.append(col)
    try:
        pipeline.data = pd.pivot_table(pipeline.data, index=index, columns=columns, values=values, aggfunc='count')
        pipeline.data = pipeline.data.rename_axis(none_list, axis=1)
        pipeline.data = pipeline.data.reset_index()
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
            if key == "index" or key == "":
                continue
            if isinstance(key, tuple):
                key = "-".join(map(str, key))
            new_schema.append({"key": key, "format": format, "description": description,
                               "parent": "", "array_field":"", "path":"", "parent_path":""})
        pipeline.schema = new_schema
        pipeline.logger.info(f"INFO: task - aggregate is done.")
        set_task_model_values(task_obj, pipeline)
    except Exception as e:
        pipeline.logger.error(f"ERROR: task - aggregate failed with an error - {str(e)}. Setting "
                              f"pipeline status to failed")
        pipeline.model.err_msg = (f"ERROR: task - aggregate failed with an error - {str(e)}. Setting "
                              f"pipeline status to failed")
        pipeline.model.save()
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
    pipeline.logger.info(f"INFO: set pipeline status to - 'In progress'")
    print(pipeline.model.status)
    pipeline.model.save()
    tasks_objects = pipeline._commands
    func_names = get_task_names(tasks_objects)
    contexts = get_task_contexts(tasks_objects)
    try:
        for i in range(len(func_names)):
            globals()[func_names[i]](contexts[i], pipeline, tasks_objects[i])
    except Exception as e:
        pipeline.model.err_msg = str(e)
        pipeline.model.save()
        raise e
    # if any of the tasks is failed, set pipeline status as - Failed
    for each_task in tasks_objects:
        if each_task.status == "Failed":
            pipeline.model.status = "Failed"
            pipeline.model.save()
            pipeline.logger.info(f"INFO: There was a failed task hence, setting pipeline status to failed")
            break
    # if none of the tasks failed, set pipeline status as - Done
    if pipeline.model.status != "Failed":
        pipeline.model.status = "Done"
        pipeline.model.save()
        pipeline.logger.info(f"INFO: All tasks were successful, setting pipeline status to Done")
    pipeline.model.output_id = str(pipeline.model.pipeline_id) + "_" + pipeline.model.status
    print("Data after pipeline execution\n", pipeline.data)
    pipeline.model.save()
    return
