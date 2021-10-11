import pandas as pd

from pipeline.task import Task
from datatransform import models


class Pipeline(object):
    def __init__(self, model: models.Pipeline, data: pd.DataFrame):
        self.model = model
        self.data = data
        self._commands = list()

    def add(self, command: Task):
        command.set_pipeline_out(self.model.output_id)
        if len(self._commands) != 0:
            self._commands[-1].add_next(command)
        self._commands.append(command)
        return self

    def execute(self):
        self.model.status = 'In Progress'
        self.model.save()
        self._commands[0].set_data(self.data)
        self._commands[0].execute_chain()
        self.model.status = "Done"
        self.model.save()
