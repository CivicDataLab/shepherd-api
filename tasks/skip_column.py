from pipeline.task import Task


class SkipColumn(Task):
    def __init__(self, model, columns):
        super().__init__(model)
        self.columns = columns

    def _execute(self):
        self.data = self.data.drop(self.columns, axis=1)
