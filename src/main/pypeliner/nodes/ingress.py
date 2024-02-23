from typing import NoReturn
from ..cores.extractor import ExtractorCore
from .abstract import AbstractNode

class IngressNode(AbstractNode):
    def __init__(self, extractor_core: ExtractorCore) -> NoReturn:
        super().__init__(extractor_core)

    def run(self):
        self.output = (record for record in self.core)
        return self.output

    ############################################################
    ####### Node Operations
    ############################################################

    def __rshift__(self, other: AbstractNode):
        other.input = self.run()
        return other

    def __lshift__(self, other: AbstractNode):
        # TARGET: self
        raise NotImplementedError("IngressNode should not be linked into.")
    
    
    # RSH1: A >> B -> B
    # RSH2: B >> A -> A
    # LSH1: A << B -> A
    # LSH2: B << A -> B
