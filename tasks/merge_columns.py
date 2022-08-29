# from pipeline.task import Task
#
#
# class MergeColumns(Task):
#     def __init__(self, model, column1, column2, separator, output_column):
#         super().__init__(model)
#         self.separator = separator
#         self.output_column = output_column
#         self.column2 = column2
#         self.column1 = column1
#
#     def _execute(self):
#
#         self.data[self.output_column] = self.data[self.column1].astype(str) + self.separator + self.data[self.column2].astype(str)
#         self.data = self.data.drop([self.column1, self.column2], axis=1)
#
