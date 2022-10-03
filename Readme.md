# Overview
This is a django based application to create and run the data-pipelines. The application runs [Prefect](https://docs.prefect.io/) tasks. Eventually, the requests made via API are converted to corresponding Prefect tasks. The tasks' status, history etc. can be monitored in the Prefect cloud by running - `prefect orion start` and opening the url shown in the prompt. 


## Background tasks 
This application needs several background tasks to be running. 
Following are the must-to run processes before running the application.
1. `python manage.py runserver ` - This starts the django server, and it listens to the API requests. This can be considered an entry-point to our program. 
2. `python manage.py process_tasks --queue create_pipeline` - Runs the **create-pipeline** background task. Request from the `runserver` is received by this task. 
3. `python manage.py runscript worker_demon.py` - Runs the rabbitmq - worker demon. 

## Demo Request
A demo request to the shepherd API consists the following in the request body. 

```{
  "pipeline_name": "Skip_merge_anonymize on Res.271",
  "res_id" : 271,
  "db_action":"create",
  "transformers_list" : [{"name" : "skip_column", "order_no" : 1, "context": {"columns":["format"]}},
                        {"name" : "merge_columns", "order_no" : 2, "context": {"column1":"title", "column2":"price", "output_column":"title with price", 
                        "separator":"|"}},
                        {"name" : "anonymize", "order_no" : 3, "context": {"to_replace" : "Sir", 
  "replace_val": "Prof", "column": "author"}}
                        ]
}
```
1. _pipeline_name_ - Name of the pipeline that needs to be created.
2. _res_id_ - Resource ID aka the ID of the resource that needs transformation. This can be considered input data for our pipeline. 
3. _db_action_ - Takes either **create** or **update**. This tells us whether to **create** a new resource in our db out of transformed data or to **update** the existing resource with the transformed data.
4. _transformers_list_ - List of json objects.
   1. _name_ - Name of the task that needs to be performed.
   2. _order_no_ - The order number of the task. In the above example - _skip_column_ is followed by _merge_columns_ which is followed by _anonymize_ tasks as the order numbers of the corresponding tasks are 1, 2 and 3 respectively.
   3. _context_ - Necessary inputs to perform the task. This is task specific. 
## Adding new tasks to the pipeline
Following are the steps to be followed to add a new task to the pipeline. 
1. Define your task name and the context (i.e. necessary information to perform the task).
2. Write the task(i.e. your Python function) in [prefect_tasks](tasks/prefect_tasks.py) file as a prefect task. 
 Note: Prefect task is a Python function annotated with `@task`. Make sure you have the same arguments passed to your function as other tasks defined in the file.