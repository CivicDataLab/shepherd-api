import abc


class Task(abc.ABC):

    def __init__(self):
        self.next_task = None
        self.shared_resources = {}

    @abc.abstractmethod
    def _execute(self):
        pass

    def execute(self):
        self._forward_resources()
        self._execute()

    def execute_chain(self):
        self.execute()
        # Upload to CKAN and update status
        self.execute_next()

    def share_next(self, key, resource):
        if self.next_task is not None:
            self.next_task._set_shared_resource(key, resource)
        return

    def _forward_resources(self):
        if self.next_task is not None:
            self.next_task._set_shared_resources(self.shared_resources)

    def _set_shared_resource(self, key, resource):
        self.shared_resources[key] = resource

    def _set_shared_resources(self, resources):
        self.shared_resources = resources

    def execute_next(self):
        if self.next_task is not None:
            self.next_task.execute_chain()
        return

    def add_next(self, task):
        self.next_task = task
