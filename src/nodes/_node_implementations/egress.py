from ...types import *
from ...types.custom import * 
from ...cores import EgressCore
from .abtract import AbstractNode

class EgressNode(AbstractNode):
    def __init__(self, loader_core: EgressCore) -> Void:
        super().__init__(loader_core)

    def run(self):
        self.core.run()