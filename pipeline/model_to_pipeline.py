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
    data = Pipeline.objects.get(pk=pipeline_id)
    try:
        data = None
        try:
            data = pd.read_pickle(data_pickle)
            os.remove(data_pickle)
        except:
            pass
        print(" got pipeline id...", pipeline_id)
        pipeline_object = Pipeline.objects.get(pk=pipeline_id)
        tasks = pipeline_object.task_set.all().order_by("order_no")
        new_pipeline = pipeline.Pipeline(pipeline_object, data)
        print("received tasks from POST request..for..", new_pipeline.model.pipeline_name)

        def execution_from_model(task):
            new_pipeline.add(task)

        [execution_from_model(task) for task in tasks]
        prefect_tasks.pipeline_executor(new_pipeline)  # pipeline_executor(task.task_name, context)
        # if db_action == "update":
        #     update_resource(
        #     {'package_id': new_pipeline.model.output_id, 'resource_name': new_pipeline.model.pipeline_name,
        #      'res_details': res_details, 'data': new_pipeline.data, 'schema': new_pipeline.schema})
        # if db_action == "create":
        #     print("scs after popping...\n")
        #     for sc in new_pipeline.schema:
        #         sc.pop('id', None)
        #
        #     for schema in new_pipeline.schema:
        #         # if found a schema with no key, no need to use it while creating
        #         if len(schema.get("key")) == 0:
        #             new_pipeline.schema.remove(schema)
        #     id = create_resource(
        #         {'package_id': new_pipeline.model.output_id, 'resource_name': new_pipeline.model.pipeline_name,
        #          'res_details': res_details, 'data': new_pipeline.data, 'schema': new_pipeline.schema}
        #     )
        #     print("res_id created at...",id)
        return

    except Exception as e:
        raise e
