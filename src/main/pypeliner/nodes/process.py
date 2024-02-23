from typing import NoReturn
from ..cores.processor import ProcessorCore
from .abstract import AbstractNode


class ProcessorNode(AbstractNode):
    def __init__(self, processor_core: ProcessorCore) -> NoReturn:
        super().__init__(processor_core)

    def run(self):
        self.output = (self.core.process(record) for record in self.input)
        return self.output

    ############################################################
    # Node Operations
    ############################################################

    def __rshift__(self, other: AbstractNode):
        other.input = self.run()
        return other

    def __lshift__(self, other: AbstractNode):
        # TARGET: self
        self.input = other.run()
        return self