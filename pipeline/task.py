import abc

import pandas as pd
from utils import upload_resource

class Task(abc.ABC):

    def __init__(self, model):
        self.next_task = None
        self.pipeline_out = ""
        self.data: pd.DataFrame = pd.DataFrame()
        self.model = model
        self.shared_resources = {}

    @abc.abstractmethod
    def _execute(self):
        pass

    def execute(self):
        self._forward_resources()
        self._execute()

    def execute_chain(self):
        self.model.status = "In Progress"
        self.model.save()
        try:
            self.execute()
        except Exception as e:
            self.model.status = "Failed with error:" + e.message
            self.model.save()
            raise e
        # print({'package_id': self.pipeline_out, 'resource_name': self.model.task_name, 'data': self.data})
        self.model.output_id = upload_resource({'package_id': self.pipeline_out, 'resource_name': self.model.task_name, 'data': self.data})

        self.model.status = "Done"
        self.model.save()
        self.execute_next()

    def set_data(self, data: pd.DataFrame):
        self.data = data

    def share_next(self, key, resource):
        if self.next_task is not None:
            self.next_task._set_shared_resource(key, resource)
        return
    
    def set_pipeline_out(self, id):
        self.pipeline_out = id

    def _forward_resources(self):
        if self.next_task is not None:
            self.next_task._set_shared_resources(self.shared_resources)

    def _set_shared_resource(self, key, resource):
        self.shared_resources[key] = resource

    def _set_shared_resources(self, resources):
        self.shared_resources = resources

    def execute_next(self):
        if self.next_task is not None:
            self.next_task.set_data(self.data)
            self.next_task.execute_chain()
        return

    def add_next(self, task):
        self.next_task = task
