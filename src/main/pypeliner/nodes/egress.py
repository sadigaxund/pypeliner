from typing import NoReturn
from ..cores.loader import LoaderCore
from .abstract import AbstractNode


class EgressNode(AbstractNode):
    def __init__(self, loader_core: LoaderCore) -> NoReturn:
        super().__init__(loader_core)

    def run(self):
        for record in self.input:
            self.core.load(record)
        
        return True

    ############################################################
    # Node Operations
    ############################################################

    def __rshift__(self, other: AbstractNode):
        raise NotImplementedError("EgressNode should not be linked from.")

    def __lshift__(self, other: AbstractNode):
        # TARGET: self
        self.input = other.run()
        return self

    # RSH1: A >> B -> B
    # RSH2: B >> A -> A
    # LSH1: A << B -> A
    # LSH2: B << A -> B
