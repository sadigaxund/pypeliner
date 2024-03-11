from ...cores import ProcessCore
from .abtract import AbstractNode

# TYPING
from typing import NoReturn

class ProcessNode(AbstractNode):
    def __init__(self, processor_core: ProcessCore) -> NoReturn:
        super().__init__(processor_core)
    @property
    def output(self):
        # SIMPLER: return map(self.core.process, self.input)
        return map(lambda record: self.core.process(record=record), self.input)
