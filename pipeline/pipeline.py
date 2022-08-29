import pandas as pd

from datatransform import models
from datatransform.models import Task


class Pipeline(object):
    def __init__(self, model: models.Pipeline, data: pd.DataFrame):
        self.model = model
        self.data = data
        self._commands = list()

    def add(self, tasks):
        self._commands.append(tasks)
        return self


    #
    # def execute(self):
    #     self.model.status = 'In Progress'
    #     self.model.save()
    #     self._commands[0].set_data(self.data)
    #     self._commands[0].execute_chain()
    #     self.model.status = "Done"
    #     self.model.save()
