from background_task import background
from datatransform.models import Task, Pipeline
from concurrent.futures import ThreadPoolExecutor
from pipeline import pipeline

mod = __import__('tasks.example_task', fromlist=['ExampleTask'])
klass = getattr(mod, 'ExampleTask')


@background(schedule=2)
def model_to_pipeline(pipeline_id):
    try:
        pipeline_object = Pipeline.objects.get(pk=pipeline_id)
        new_pipeline = pipeline.Pipeline()
        new_pipeline.add(klass())
        with ThreadPoolExecutor(max_workers=1) as executor:
            _ = executor.submit(new_pipeline.execute)

    except Exception as e:
        raise e
