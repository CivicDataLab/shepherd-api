# Overview
This is a django based application to create and run the data-pipelines. The application runs [Prefect](https://docs.prefect.io/) tasks. Eventually, the requests made via API are converted to corresponding Prefect tasks. The tasks' status, history etc. can be monitored in the Prefect cloud by running - `prefect orion start` and opening the url shown in the prompt. 


## Requirements
- Once the code is cloned from the git, install the requirements from `requirements.txt` file by running `pip install -r requirements.txt`
- The project uses rabbitmq, which can be installed from the [official website](https://www.rabbitmq.com/download.html)

## Background tasks 
This application needs several background tasks to be running. 
Following are the must-to run processes before running the application. Run all these processes parallely in different terminals. 
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
  "replace_val": "Prof", "column": "author"}}]
}
```
1. _pipeline_name_ - Name of the pipeline that needs to be created.
2. _res_id_ - Resource ID i.e. ID of the resource that needs transformation. This can be considered input data for our pipeline. 
3. _db_action_ - Takes either **create** or **update**. This tells us whether to **create** a new resource in our db out of transformed data or to **update** the existing resource with the transformed data.
4. _transformers_list_ - List of json objects.
   1. _name_ - Name of the task that needs to be performed.
   2. _order_no_ - The order number of the task. In the above example - _skip_column_ is followed by _merge_columns_ which is followed by _anonymize_ tasks as the order numbers of the corresponding tasks are 1, 2 and 3 respectively.
   3. _context_ - Necessary inputs to perform the task. This is task specific. 

The final HTTP request looks like following.
```
POST http://127.0.0.1:8000/transformer/res_transform
Content-Type: application/json
{
    "pipeline_name": "Skip_merge_anonymize on Res.271",
    "res_id" : 271,
    "db_action":"create",
    "transformers_list" : [{"name" : "skip_column", "order_no" : 1, "context": {"columns":["format"]}},
                        {"name" : "merge_columns", "order_no" : 2, "context": {"column1":"title", "column2":"price", "output_column":"title with price", 
                        "separator":"|"}},
                        {"name" : "anonymize", "order_no" : 3, "context": {"to_replace" : "Sir", 
  "replace_val": "Prof", "column": "author"}}]
}
```
## Adding new tasks to the pipeline
Following are the steps to be followed to add a new task to the pipeline. 
1. Define your task name and the context (i.e. necessary information to perform the task).
2. Write the task(i.e. your Python function) in [prefect_tasks](tasks/prefect_tasks.py) file as a prefect task. 
 Note: Prefect task is a Python function annotated with `@task`. Make sure you have the same arguments passed to your function as other tasks defined in the file.

Let's understand this through an example. Suppose you need to add a task named - **add_prefix** which adds a given prefix to all the values in the specified column
So, the task name would be - add_prefix. To define the context, let's define the necessary inputs first. 
- We will be needing the column name. So, the context should contain a key named 'column'
- We will also be needing a string which acts as prefix. So the second key in the context should be - 'prefix'

Finally, our context should look something like this. 

`"context" : {"column": "<column_name_here>", "prefix":"<prefix_string_here>"}`

Now we should add the task in [prefect_tasks](tasks/prefect_tasks.py) file. Go to the file, and add the task.
```
@task
def add_prefix(context, pipeline, task_obj):
    column = context['column']
    prefix_string = context['prefix']
    # Rest of your logic here..
```
A request to create a pipeline with this task should look something like,
```
POST http://127.0.0.1:8000/transformer/res_transform
Content-Type: application/json
{
    "pipeline_name": "Test_prefixing",
    "res_id" : 271,
    "db_action":"create",
    "transformers_list" : [
        {"name" : "add_prefix", 
        "order_no" : 1, 
        "context": {"column": "Planets", "prefix": "The"}}
        ]
}
```
