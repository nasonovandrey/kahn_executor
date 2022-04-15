from time import strftime, gmtime
from graph import Graph, run_batch

class Task:
    def __init__(self, name, func, *args, **kwargs):
        self._name = name
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._tasks = {}
    
    def depends_on(self, *args):
        self._tasks = set(args)
        return self

    def __repr__(self):
        return 'Task(\''+self._name+'\','+repr(self._func)+','+repr(self._args)+')'
    
    @property
    def previous(self):
        return self._tasks

    def execute(self):
        print('Running task',self._name,'at',strftime('%Y-%m-%dT%H:%M:%S', gmtime()))
        self._func(*self._args, **self._kwargs)
        print('Task',self._name,'finished at',strftime('%Y-%m-%dT%H:%M:%S', gmtime()))

    def reactive_execute(self, concurrency=1):
        execution_plan = Graph(self).layer_sort()
        for layer in execution_plan:
            run_batch(layer, concurrency)
