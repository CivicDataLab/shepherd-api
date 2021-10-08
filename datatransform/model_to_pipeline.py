from .models import Task, Pipeline
from concurrent.futures import ThreadPoolExecutor
from pipeline import pipeline

# module = "tasks.example_task.ExampleTask"
# task = __import__(module)

mod = __import__('tasks.example_task', fromlist=['ExampleTask'])
klass = getattr(mod, 'ExampleTask')


def model_to_pipeline(p: Pipeline):
    try:
        new_pipeline = pipeline.Pipeline()
        new_pipeline.add(klass())
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(new_pipeline.execute)
            print(future.result())

    except Exception as e:
        raise e
