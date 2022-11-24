import os
import random
import re

import pandas as pd
import pdfkit
from json2xml import json2xml
from pandas.io.json import build_table_schema
from prefect import task, flow
from task_utils import *


@task(name="json_skip_col")
def skip_column(context, pipeline, task_obj):
    column = context['columns']
    col = column
    if not isinstance(column, list):
        column = list()
        column.append(col)
    try:
        def remove_a_key(d, remove_key):
            for key in list(d.keys()):
                if key in remove_key:
                    del d[key]
                else:
                    skip_col(d[key], remove_key)

        def skip_col(d, remove_key):
            if isinstance(d, dict):
                remove_a_key(d, remove_key)
            if isinstance(d, list):
                for each in d:
                    if isinstance(each, dict):
                        remove_a_key(each, remove_key)
            return d
        pipeline.data = skip_col(pipeline.data, column)
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


@task(name="json_anonymize")
def anonymize(context, pipeline, task_obj):
    option = context['option']
    special_char = context['special_char']
    col = context['column']
    try:
        def anonymize_a_key(d, change_key):
            for key in list(d.keys()):
                if key == change_key:
                    print("got change key..")
                    val = d[key]
                    print("value type is...", type(val))
                    if isinstance(val, str):
                        if option == "replace_all":
                            if special_char == "random":
                                replace_val = "".join(random.choices("!@#$%^&*()<>?{}[]~`", k=len(val)))
                                d[key] = replace_val
                            else:
                                replace_val = special_char * len(val)
                                d[key] = replace_val
                        elif option == "replace_nth":
                            n = context.get('n')
                            n = int(n) - 1
                            if special_char == "random":
                                replacement = "".join(random.choices("!@#$%^&*()<>?{}[]~`", k=1))
                                replace_val = val[0:int(n)] + replacement + val[int(n) + 1:]
                                d[key] = replace_val
                            else:
                                replace_val = val[0:int(n)] + special_char + val[int(n) + 1:]
                                d[key] = replace_val
                        elif option == "retain_first_n":
                            n = context.get('n')
                            if special_char == "random":
                                replacement = "".join(random.choices("!@#$%^&*()<>?{}[]~`", k=(len(val) - int(n))))
                                replace_val = val[:int(n)] + replacement
                                d[key] = replace_val
                            else:
                                replace_val = val[:int(n)] + (special_char * (len(val) - int(n)))
                                d[key] = replace_val
                else:
                    anonymize_col(d[key], change_key)

        def anonymize_col(d, change_key):
            if isinstance(d, dict):
                anonymize_a_key(d, change_key)
            if isinstance(d, list):
                for each in d:
                    if isinstance(each, dict):
                        anonymize_a_key(each, change_key)

            return d
        pipeline.data = anonymize_col(pipeline.data, col)
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


@task(name="json_merge_columns")
def merge_columns(context, pipeline, task_obj):
    column1, column2, output_column = context['column1'], context['column2'], context['output_column']
    retain_cols = False
    try:
        retain_cols = context['retain_cols']
    except:
        pass
    separator = context['separator']
    columns_list = [column1, column2]
    try:
        def merge_cols(d, cols_list, result):
            for key in list(d.keys()):
                if key in cols_list:
                    result.append(str(d[key]))
                else:
                    merge_col(d[key], cols_list, result)

        def merge_col(d, cols_list, result):
            if isinstance(d, dict):
                print("got a dict..")
                merge_cols(d, cols_list, result)
            if isinstance(d, list):
                for each in d:
                    if isinstance(each, dict):
                        val_list = []
                        for key in list(each.keys()):
                            if key in cols_list:
                                val_list.append(str(each[key]))
                                if not retain_cols:
                                    del each[key]
                                if len(val_list) == 2:
                                    result_str = str(separator).join(val_list)
                                    each[output_column] = result_str
                                    pipeline.merge_flag = True
                        merge_cols(each, cols_list, result)
        result = []
        merge_col(pipeline.data, columns_list, result)
        if isinstance(pipeline.data, dict) and not pipeline.merge_flag:
            pipeline.data[output_column] = result
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
            "key": output_column, "format": "string",
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


@task(name="json_change_format")
def change_format(context, pipeline, task_obj):
    file_format = context['format']
    result_file_name = pipeline.model.pipeline_name
    dir = "format_changed_files/"
    json_str = json.dumps(pipeline.data)
    json_data = pd.read_json(json_str, orient="index")
    csv_data = json_data.to_csv()
    if file_format == "csv" or file_format =="CSV":
        try:
            with open(dir+result_file_name + '.csv', 'w') as f:
                f.write(csv_data)
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
    if file_format == "xml" or file_format =="XML":
        try:
            json_data = json_data.to_json(orient="records")
            json_data = json.loads(json_data)
            xml_data = json2xml.Json2xml(json_data).to_xml()
            with open(dir + result_file_name + '.xml', 'w') as f:
                f.write(xml_data)
            pipeline.logger.info(f"INFO: Resource format changed to xml")
            set_task_model_values(task_obj, pipeline)
        except Exception as e:
            pipeline.logger.error(f"ERROR: task - change_format failed with an error - {str(e)}. Setting "
                          f"pipeline status to failed")
            send_error_to_prefect_cloud(e)
            task_obj.status = "Failed"
            task_obj.save()
    elif file_format == "pdf" or file_format == "PDF":
        try:
            json_data.to_html('data.html')
            pdfkit.from_file("data.html", dir + result_file_name + ".pdf")
            os.remove('data.html')
            pipeline.logger.info(f"INFO: Resource format changed to pdf")
            set_task_model_values(task_obj, pipeline)
        except Exception as e:
            pipeline.logger.error(f"ERROR: task - change_format failed with an error - {str(e)}. Setting "
                                  f"pipeline status to failed")
            send_error_to_prefect_cloud(e)
            task_obj.status = "Failed"
            task_obj.save()


@flow
def json_pipeline_executor(pipeline):
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

