import json
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from prefect import task, flow

from utils import upload_resource, create_resource


@task
def skip_column(context, pipeline, task):
    column = context['columns']
    try:
        pipeline.data = pipeline.data.drop(column, axis=1)
    except Exception as e:
        print("Problem " + str(e) + " while executing skip_column task. Retrying...")
    set_task_model_values(task, pipeline)


@task
def merge_columns(context, pipeline, task):
    column1, column2, output_column = context['column1'], context['column2'], context['output_column']
    separator = context['separator']
    try:
        pipeline.data[output_column] = pipeline.data[column1].astype(str) + separator + pipeline.data[column2] \
            .astype(str)
        pipeline.data = pipeline.data.drop([column1, column2], axis=1)
    except Exception as e:
        print("Problem " + str(e) + " while executing merge_columns task. Retrying...")
    set_task_model_values(task, pipeline)


@flow
def pipeline_executor(pipeline):
    pipeline.model.status = "In Progress"
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
    task.output_id = create_resource(
        {'package_id': pipeline.model.output_id, 'resource_name': task.task_name, 'data': pipeline.data})
    print({'package_id': task.output_id, 'resource_name': task.task_name, 'data': pipeline.data})
    task.status = "Done"
    task.save()
