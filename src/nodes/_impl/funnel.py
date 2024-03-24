from ...cores import FunnelCore
from .abtract import AbstractNode
import itertools

# TYPING
from typing import (
    Iterable,
    Any,
    NoReturn,
    Generator,
    List
)

def conjoined_enumerate(generators) -> Generator[List, None, None]:
    """_summary_
    Example Use Case:
    Aggregate:  +
    Inputs:     [1,2], [3,0, 99], []
    Output:     [1 + 3 + None, 2 + 0 + None, None + 99 + None]
    
    Args:
        generators (_type_): _description_

    Yields:
        Generator[List, None, None]: _description_
    """
    while True:
        next_elements = []
        all_done = True
        for gen in generators:
            try:
                next_element = next(gen)
                next_elements.append(next_element)
                all_done = False
            except StopIteration:
                next_elements.append(None)
        if all_done or all(elem is None for elem in next_elements):
            break
        
        yield next_elements

class FunnelNode(AbstractNode): 
    def __init__(self, funnel_core: FunnelCore) -> NoReturn:
        self.input_flows = list()
        super().__init__(funnel_core)

    @property
    def output(self):
        conjoined_flows = conjoined_enumerate(self.input_flows)
        return map(lambda flow: self.core.aggregate(*flow), conjoined_flows)

    ############################################################
    # Node Operations
    ############################################################

    @property
    def input(self) -> List[Iterable]:
        return self.input_flows
    
    @input.setter
    def input(self, new_input: Iterable | Any):
        if not isinstance(new_input, Iterable):
            new_input = [new_input]
            
        self.input_flows.append(itertools.chain(new_input))
        

    def __add__(self, other: AbstractNode):
        self.input = other.output
        return self
