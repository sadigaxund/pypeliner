from ...cores import JunctionCore
from .abtract import AbstractNode, Shiftable
from collections import deque

import itertools

# TYPING
from typing import (
    Iterable,
    NoReturn,
)

class Outflow:
    def __init__(self, source: Iterable, outputs:int=2) -> None:
        self.source = source
        self.outputs = [deque() for _ in range(outputs)]

    def iterate(self):
        values = next(self.source)
        for i, value in enumerate(values):
            self.outputs[i].append(value)

    def next(self, i):
        if len(self.outputs[i]) == 0:
            self.iterate()

        return self.outputs[i].popleft()


class Flow(Shiftable):
    def __init__(self, outflow: Outflow, index: int) -> None:
        self.outflow = outflow
        self.index = index

    def __iter__(self):
        return self

    def __next__(self):
        return self.outflow.next(self.index)
    
    @property
    def output(self) -> Iterable:
        return self



class JunctionNode(AbstractNode):
    def __init__(self, junction_core: JunctionCore, outflows = 2, input = None) -> NoReturn:
        self.outflows = outflows
        self.input = input
        super().__init__(junction_core)
        
    def __getitem__(self, key):
        return Flow(self._input, key)
    
    @property
    def input(self):
        return super().input
    
    @input.setter
    def input(self, new_input):
        if not isinstance(new_input, Iterable):
            new_input = [new_input]
            
        self._input = itertools.chain(new_input)
        self._input = map(lambda record: self.core.segregate(record), self._input)
        self._input = Outflow(self._input, self.outflows)
    
    def __rshift__(self, other: 'AbstractNode'):
        raise TypeError("JunctionNode can only pass output from it's slices.")        
    
   