import json

from prefect import get_run_logger


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