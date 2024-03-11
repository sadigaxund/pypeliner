from ...types import *
from ...types.custom import * 
from ...cores import EgressCore
from .abtract import AbstractNode

class EgressNode(AbstractNode):
    def __init__(self, egress_core: EgressCore) -> Void:
        super().__init__(egress_core)

    def run(self):
        self.core.run()
        
    @property
    def input(self):
        return self.core.input
    
    @input.setter
    def input(self, new_input):
        self.core.input = new_input