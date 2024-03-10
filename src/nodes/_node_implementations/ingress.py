from ...types import *
from ...types.custom import * 
from ...cores import IngressCore
from .abtract import AbstractNode

class IngressNode(AbstractNode):
    def __init__(self, extractor_core: IngressCore) -> Void:
        super().__init__(extractor_core)
    
    def run(self):...
            
    @property
    def input(self):
        return self.core.input
    
    @input.setter
    def input(self, new_input):
        self.core.input = new_input

    @property
    def output(self):
        return self.core

    def __rshift__(self, other: AbstractNode):
        other.input = self.output
        return other