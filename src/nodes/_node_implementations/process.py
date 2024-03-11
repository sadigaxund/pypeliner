from ...types import *
from ...types.custom import * 
from ...cores import ProcessCore
from .abtract import AbstractNode

class ProcessNode(AbstractNode):
    def __init__(self, processor_core: ProcessCore) -> Void:
        super().__init__(processor_core)
    def run():...
    @property
    def output(self):
        # SIMPLER: return map(self.core.process, self.input)
        return map(lambda record: self.core.process(record=record), self.input)
