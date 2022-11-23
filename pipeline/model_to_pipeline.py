import json
import os
from datatransform.models import Pipeline
from pipeline import pipeline
from config import settings
import pandas as pd
from tasks import prefect_tasks
from utils import update_resource, create_resource

mod = __import__('tasks', fromlist=settings.tasks.values())


def task_executor(pipeline_id, data_pickle, res_details, db_action):
    print("inside te***")
    print("pipeline_id is ", pipeline_id)
    try:
        data = None
        try:
            data = pd.read_pickle(data_pickle)
            os.remove(data_pickle)
        except:
            pass
        print(" got pipeline id...", pipeline_id)
        pipeline_object = Pipeline.objects.get(pk=pipeline_id)
        task = list(pipeline_object.task_set.all().order_by("order_no"))[-1]
        new_pipeline = pipeline.Pipeline(pipeline_object, data)
        print("received tasks from POST request..for..", new_pipeline.model.pipeline_name)
        # def execution_from_model(task):
        #     new_pipeline.add(task)
        if getattr(task, "status") == "Created":
            new_pipeline.add(task)
        print(new_pipeline._commands)
        # if res_details == "api_res":
        #     prefect_tasks.pipeline_executor(new_pipeline)
        #     return new_pipeline.data
        # new_pipeline.schema = res_details['data']['resource']['schema']
        # prefect_tasks.pipeline_executor(new_pipeline)
        # if new_pipeline.model.status == "Failed":
        #     raise Exception("There was an error while running the pipeline")
        # if db_action == "update":
        #     fresh_schema = []
        #     for schema in new_pipeline.schema:
        #         if len(schema['key']) != 0:
        #             fresh_schema.append(schema)
        #     new_pipeline.schema = fresh_schema
        #     update_resource(
        #         {'package_id': new_pipeline.model.output_id, 'resource_name': new_pipeline.model.pipeline_name,
        #          'res_details': res_details, 'data': new_pipeline.data, 'schema': new_pipeline.schema,
        #          "logger": new_pipeline.logger})
        # if db_action == "create":
        #     for sc in new_pipeline.schema:
        #         sc.pop('id', None)
        #
        #     fresh_schema = []
        #     for schema in new_pipeline.schema:
        #         # if found a schema with no key, no need to use it while creating
        #         if len(schema['key']) != 0:
        #             fresh_schema.append(schema)
        #     new_pipeline.schema = fresh_schema
        #     id = create_resource(
        #         {'package_id': new_pipeline.model.output_id, 'resource_name': new_pipeline.model.pipeline_name,
        #          'res_details': res_details, 'data': new_pipeline.data, 'schema': new_pipeline.schema,
        #          "logger": new_pipeline.logger}
        #     )
        #     print("res_id created at...", id)
        return

    except Exception as e:
        raise e
