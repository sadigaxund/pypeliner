from ...cores import IngressCore
from .abtract import AbstractNode

# TYPING
from typing import (
    Iterable,
    Any,
    NoReturn,
)

class IngressNode(AbstractNode):
    def __init__(self, ingress_core: IngressCore) -> NoReturn:
        super().__init__(ingress_core)
    
    @property
    def input(self) -> Iterable:
        return self.core.input
    
    @input.setter
    def input(self, new_input: Iterable | Any):
        self.core.input = new_input

    @property
    def output(self):
        return self.core