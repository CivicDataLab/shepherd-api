import sys
import time
import os
# sys.path.append('dataplatform')
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dataplatform.settings")
# import django
# django.setup()
import pandas as pd
from datatransform.models import Task, Pipeline
# from pipeline import pipeline
from config import settings

from tasks import prefect_tasks

mod = __import__('tasks', fromlist=settings.tasks.values())

print("inside te***")
data = Pipeline.objects.get(pk=1)
print(data.pipeline_name)

# def task_executor(pipeline_id, data_pickle):
#     print("inside te***")
#     data = Pipeline.objects.get(pk=pipeline_id)
#     print(data.pipeline_name)
    # try:
    #     data = None
    #     try:
    #         data = pd.read_pickle(data_pickle)
    #         os.remove(data_pickle)
    #     except:
    #         pass
        # print(" got pipeline id...", pipeline_id)
        # pipeline_object = Pipeline.objects.get(pk=pipeline_id)
        # tasks = pipeline_object.task_set.all().order_by("order_no")
        # new_pipeline = pipeline.Pipeline(pipeline_object, data)
        # print("received tasks from POST request..for..", new_pipeline.model.pipeline_name)
        #
        # # for i in tasks:
        # #     print(i.task_name)
        # def execution_from_model(task):
        #     new_pipeline.add(task)
        #
        # [execution_from_model(task) for task in tasks]
        # print("data recieved\n", new_pipeline.data)
        # prefect_tasks.pipeline_executor(new_pipeline)  # pipeline_executor(task.task_name, context)
        # return

    # except Exception as e:
    #     raise e
