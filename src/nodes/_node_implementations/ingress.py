from ...types import *
from ...types.custom import * 
from ...cores import IngressCore
from .abtract import AbstractNode

class IngressNode(AbstractNode):
    def __init__(self, extractor_core: IngressCore) -> Void:
        super().__init__(extractor_core)

    @property
    def input(self):
        return super().input
    
    @input.setter
    def input(self, new_input):
        self.core.input = new_input

    ############################################################
    # Node Operations
    ############################################################

    def __rshift__(self, other: AbstractNode):
        try:
            self.output
        except NotImplementedError:
            self.run()
        finally:
            other.input = self.output
        return other