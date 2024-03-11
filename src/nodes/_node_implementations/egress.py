# TOOLS
from .abtract import AbstractNode
from ...cores import EgressCore

# TYPING
from typing import (
    Iterable,
    Any,
    NoReturn,
    Iterable
)

class EgressNode(AbstractNode):
    def __init__(self, egress_core: EgressCore) -> NoReturn:
        super().__init__(egress_core)

    def run(self):
        self.core.run()
        
    @property
    def input(self) -> Iterable:
        return self.core.input
    
    @input.setter
    def input(self, new_input: Iterable | Any):
        self.core.input = new_input