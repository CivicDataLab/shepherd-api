from background_task import background
from datatransform.models import Task, Pipeline
from concurrent.futures import ThreadPoolExecutor
from pipeline import pipeline
from config import settings

mod = __import__('tasks', fromlist=settings.tasks.values())


@background(schedule=2)
def model_to_pipeline(pipeline_id):
    try:
        pipeline_object = Pipeline.objects.get(pk=pipeline_id)
        tasks = pipeline_object.task_set.all().order_by("order_no")
        new_pipeline = pipeline.Pipeline()

        def execution_from_model(task):
            klass = getattr(mod, settings.tasks[task.task_name])
            new_pipeline.add(klass())

        [execution_from_model(task) for task in tasks]
        with ThreadPoolExecutor(max_workers=1) as executor:
            _ = executor.submit(new_pipeline.execute)

    except Exception as e:
        raise e
