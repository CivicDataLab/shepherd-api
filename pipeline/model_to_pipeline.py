import json
import os
from datatransform.models import Pipeline
from pipeline import pipeline
from config import settings
import pandas as pd
from tasks import prefect_tasks
from utils import update_resource, create_resource

mod = __import__('tasks', fromlist=settings.tasks.values())

def task_executor(pipeline_id, data_pickle):
    print("inside te***")
    print("pipeline_id is ", pipeline_id)
    try:
        data = None
        try:
            data = pd.read_csv(data_pickle)
            print(data)
        except Exception as e:
            print(str(e), "error in model to pipeline!!!!!")
            pass
        finally:
            os.remove(data_pickle)
        print(" got pipeline id...", pipeline_id)
        pipeline_object = Pipeline.objects.get(pk=pipeline_id)
        tasks = pipeline_object.task_set.all().order_by("order_no")
        new_pipeline = pipeline.Pipeline(pipeline_object, data)
        print("received tasks from POST request..for..", new_pipeline.model.pipeline_name)
        print("data before...", new_pipeline.data)
        def execution_from_model(task):
            new_pipeline.add(task)

        [execution_from_model(task) for task in tasks]
        prefect_tasks.pipeline_executor(new_pipeline)  # pipeline_executor(task.task_name, context)
        return

    except Exception as e:
        raise e
