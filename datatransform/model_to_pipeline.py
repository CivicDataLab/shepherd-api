from .models import Task, Pipeline
from concurrent.futures import ThreadPoolExecutor
from pipeline import pipeline

module = "tasks.example_task.ExampleTask"
task = __import__(module)


async def model_to_pipeline(pipeline: Pipeline):
    try:
        new_pipeline = pipeline.Pipeline()
        new_pipeline.add(task.ExampleTask())
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(new_pipeline.execute)
            print(future.result())

    except Exception as e:
        raise e
