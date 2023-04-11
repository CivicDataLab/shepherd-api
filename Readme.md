# Overview
This is a django project to create and run the data-pipelines. The project contains two main parts.
1. task publisher - A [Prefect]() pipeline, which publishes the tasks in the order as received from API request(from user interface). 
2. Task executor - a demon process that executes when there's a task published for it. 

[Rabbit-mq]() is used to implement this 'publish-subscribe pattern'.  

# Project flow
![Flow chart]()

## Background tasks 
The application needs several background tasks to be running. 
Following are the must-run processes before running the application.
1. `python manage.py runserver ` - This starts the django server, and it listens to the API requests. This can be considered an entry-point to our program. 
2. `python manage.py runscript worker_demon.py` - Runs the rabbitmq - worker demon. 

## Demo Request
A demo request to the shepherd API consists the following in the request body. 

```{
  "pipeline_name": "Example-pipeline",
  "data_url" : "",
  "transformers_list" : [{"name" : "skip_column", "order_no" : 1, "context": {"columns":["format"]}},
                        {"name" : "merge_columns", "order_no" : 2, "context": {"column1":"title", "column2":"price", "output_column":"title with price", 
                        "separator":"|"}},
                        {"name" : "anonymize", "order_no" : 3, "context": {"to_replace" : "Sir", 
  "replace_val": "Prof", "column": "author"}}
                        ]
}
```
1. _pipeline_name_ - Name of the pipeline that needs to be created.
2. _data_url_ - Url to the csv data file.
3. _transformers_list_ - List of json objects.
   1. _name_ - Name of the task that needs to be performed.
   2. _order_no_ - The order number of the task. In the above example - _skip_column_ is followed by _merge_columns_ which is followed by _anonymize_ tasks as the order numbers of the corresponding tasks are 1, 2 and 3 respectively.
   3. _context_ - Necessary inputs to perform the task. This is task specific. 
## Adding new tasks to the pipeline
Following are the steps to be followed to add a new task to the pipeline. 
1. Define your task name and the context (i.e. necessary information to perform the task).
2. The name of the publisher method, and the name of the task should be same. 
3. Generate publisher code by running `python -m code_templates.publisher_template --task_name '<task_name_here>'` in a terminal.
Paste the generated code in [prefect_tasks](tasks/prefect_tasks.py)
  
``data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data)  
  if not exception_flag:``
 These lines publish your task along with the context and the data in the queue. After that write post-processing script. i.e. what you want to do with data.
4. Write the worker's logic in a separate file under [scripts](tasks/scripts) directory. Generate the code
by running `task_template.py` under - [code_templates](code_templates) directory. Following is the sample command to generate worker template

` python task_template.py --task_name "<task_name_here>" --result_file "<resultant_file_name_here>"`

The above command will generate the task-template. Go through it, and there will be a method defined with the task name and the logic needs to be written there. 

5. Once the worker code is ready, deploy it to the server, and run the code i.e. 
 `python <file_name.py>` - this will start the worker. Whenever the publisher publishes a message for this worker, worker picks the message, processes it
and returns the response back to the publisher.
