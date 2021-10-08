from pipeline.task import Task


class Pipeline(object):
    def __init__(self):
        self._commands = list()

    def add(self, command: Task):
        if len(self._commands) != 0:
            self._commands[-1].add_next(command)
        self._commands.append(command)
        return self

    def execute(self):
        self._commands[0].execute_chain()
