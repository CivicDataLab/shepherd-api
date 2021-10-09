from pipeline.task import Task


class ExampleTask(Task):
    def __init__(self):
        super().__init__()

    def _execute(self):
        f = open("asd.txt", "w")
        f.write("done")
        print("asdasd")
        f.close()
        self.share_next("key", "value")
