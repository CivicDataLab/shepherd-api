from pipeline.task import Task


class ExampleTask(Task):
    def __init__(self):
        super().__init__()


    def _execute(self):
        print("executing")
        self.share_next("key", "value")
