from pipeline.task import Task


class MergeColumns(Task):
    def __init__(self, model, column1, column2, output_column):
        super().__init__(model)
        self.output_column = output_column
        self.column2 = column2
        self.column1 = column1

    def _execute(self):

        self.data[self.output_column] = self.data[self.column1] + self.data[self.column2]
        self.data = self.data.drop([self.column1, self.column2], axis=1)

