from multiprocessing import Process
from collections import deque
from copy import deepcopy


class Graph:
    def __init__(self, node):
        self.nodes = {}
        if not hasattr(node, 'previous'):
            raise Exception('No method with name \'previous\' is defined')
        self.nodes[node] = node.previous
        to_visit = deque(node.previous)
        while len(to_visit)!=0:
            current = to_visit.popleft()
            if not current in self.nodes:
                self.nodes[current] = current.previous
                to_visit = to_visit + deque(current.previous)

    def delete_node(self, node):
        del self.nodes[node]
        for n,l in self.nodes.items():
            if node in l:
                l.remove(node)

    @property
    def initial_nodes(self):
        return [node for node, prevs in self.nodes.items() if len(prevs)==0]

    def layer_sort(self):
        graph = deepcopy(self)
        layers = []
        while graph.nodes:
            inits = graph.initial_nodes
            if len(inits)==0 and graph.nodes:
                raise Exception('Cycles in a graph')
            layers.append(inits)
            for node in inits:
                graph.delete_node(node)
        return layers


def run_batch(tasks, concurrency=1):
    for step in range(0,len(tasks),concurrency):
        processes = []
        for index in range(step, min(concurrency+step,len(tasks))):
            processes.append(Process(target=tasks[index].execute))
            processes[-1].start()
        for process in processes:
            process.join()


