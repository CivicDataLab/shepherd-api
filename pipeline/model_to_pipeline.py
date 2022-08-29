import sys
import time

import pandas as pd
import prefect
from background_task import background
from prefect import flow
import self as self
from datatransform.models import Task, Pipeline
from concurrent.futures import ThreadPoolExecutor
from pipeline import pipeline
from config import settings
import json
import os
from tasks import prefect_tasks

mod = __import__('tasks', fromlist=settings.tasks.values())


@background(schedule=1)
def model_to_pipeline(pipeline_id, data_pickle):
    time.sleep(5)
    try:
        data = None
        try:
            data = pd.read_pickle(data_pickle)
            os.remove(data_pickle)
        except:
            pass
        pipeline_object = Pipeline.objects.get(pk=pipeline_id)
        tasks = pipeline_object.task_set.all().order_by("order_no")
        new_pipeline = pipeline.Pipeline(pipeline_object, data)

        def execution_from_model(task):
            new_pipeline.add(task)

        [execution_from_model(task) for task in tasks]
        print("data recieved\n", new_pipeline.data)
        prefect_tasks.pipeline_executor(new_pipeline)  # pipeline_executor(task.task_name, context)
        return

    except Exception as e:
        raise e

