import json
import os
from datatransform.models import Pipeline
from pipeline import pipeline
from config import settings
import pandas as pd
from tasks import prefect_tasks, prefect_json_transformations
from utils import update_resource, create_resource
import requests
from configparser import ConfigParser
config = ConfigParser()

config.read("config.ini")

mod = __import__('tasks', fromlist=settings.tasks.values())


def task_executor(pipeline_id, data_pickle, res_details, db_action, file_format):
    db_action = "update"
    print("inside te***")
    print("pipeline_id is ", pipeline_id)
    data = None
    try:
        print ('******',file_format,  file_format.lower()=="csv")
        if file_format.lower() == "csv":
            print ('*--------------------', data_pickle)
            try:
                data = pd.read_csv(data_pickle)
                print(data_pickle, "?????")
                os.remove(data_pickle)
            except Exception as e:
                print ('----error', str(e))
        elif file_format.lower() == "json":
            f = open(data_pickle, "rb")
            data = json.load(f)
            f.close()
            os.remove(data_pickle)
            if isinstance(data, str):
                data = json.loads(data)
        pipeline_object = Pipeline.objects.get(pk=pipeline_id)
        new_pipeline = pipeline.Pipeline(pipeline_object, data)
        print(" got pipeline id...", pipeline_id)
        print("data before,,,%%%", data)
        if res_details == "api_res":
            # if it's API resource need to execute all the tasks at once.
            tasks = pipeline_object.task_set.all().order_by("order_no")

            def execution_from_model(task):
                new_pipeline.add(task)

            [execution_from_model(task) for task in tasks]
            if file_format.lower() == "csv":
                prefect_tasks.pipeline_executor(new_pipeline)
                return new_pipeline.data
            elif file_format.lower() == "json":
                prefect_json_transformations.json_pipeline_executor(new_pipeline)
                return new_pipeline.data

        task = list(pipeline_object.task_set.all().order_by("order_no"))[-1]

        print("received tasks from POST request..for..", new_pipeline.model.pipeline_name)

        if getattr(task, "status") == "Created":
            new_pipeline.add(task)
        print(new_pipeline._commands)

        # def execution_from_model(task):
        #     new_pipeline.add(task)

        # [execution_from_model(task) for task in tasks]

        new_pipeline.schema = res_details['data']['resource']['schema']

        if file_format == "CSV":
            prefect_tasks.pipeline_executor(new_pipeline)
        elif file_format == "JSON":
            prefect_json_transformations.json_pipeline_executor(new_pipeline)
        if new_pipeline.model.status == "Failed":
            raise Exception("There was an error while running the pipeline")
        if db_action == "update":
            fresh_schema = []
            for schema in new_pipeline.schema:
                if len(schema['key']) != 0:
                    fresh_schema.append(schema)
            new_pipeline.schema = fresh_schema
            update_resource(
                {'package_id': new_pipeline.model.output_id, 'resource_name': new_pipeline.model.pipeline_name,
                 'res_details': res_details, 'data': new_pipeline.data, 'schema': new_pipeline.schema,
                 "logger": new_pipeline.logger})
            
            api_schema_url = os.environ.get('BACKEND_URL', config.get("datapipeline", "BACKEND_URL"))  + "api_schema/" + str(res_details['data']['resource']['id']) +  "/"
            new_schema_resp = requests.get(api_schema_url)
            new_schema = new_schema_resp.json()
            new_schema = new_schema["schema"]           
            
            update_resource(
                {'package_id': new_pipeline.model.output_id, 'resource_name': new_pipeline.model.pipeline_name,
                 'res_details': res_details, 'data': new_pipeline.data, 'schema': new_schema,
                 "logger": new_pipeline.logger})            
        if db_action == "create":
            for sc in new_pipeline.schema:
                sc.pop('id', None)

            fresh_schema = []
            for schema in new_pipeline.schema:
                # if found a schema with no key, no need to use it while creating
                if len(schema['key']) != 0:
                    fresh_schema.append(schema)
            new_pipeline.schema = fresh_schema
            id = create_resource(
                {'package_id': new_pipeline.model.output_id, 'resource_name': new_pipeline.model.pipeline_name,
                 'res_details': res_details, 'data': new_pipeline.data, 'schema': new_pipeline.schema,
                 "logger": new_pipeline.logger}
            )
            new_pipeline.model.resultant_res_id = id
            new_pipeline.model.save()
            print("res_id created at...", id)
        return

    except Exception as e:
        new_pipeline.model.err_msg = str(e)
        new_pipeline.model.save()
        raise e
