from pypeliner import types as TP

from .cores import *

__all__ = [
    'AbstractNode',
    'SinkNode',
    'SourceNode',
    'ProcessNode',
    'JunctionNode',
    'FunnelNode',
]

class AbstractNode(TP.AbstractClass, metaclass=TP.AbstractMetadata):
    def __init__(self, core: AbstractCore) -> TP.Return0:
        self.core = core
        self.__input = NotImplementedError(
            "The Node implementation does not feature input capability.")
        self.__output = NotImplementedError(
            "The Node implementation does not feature output capability.")

    @TP.AbstractMethod
    def run(): ...

    @property
    def input(self):
        if isinstance(self.__input, NotImplementedError):
            raise self.__input

        return self.__input

    @property
    def output(self):
        if isinstance(self.__output, NotImplementedError):
            raise self.__output

        return self.__output

    @input.setter
    def input(self, value):
        self.__input = value

    @output.setter
    def output(self, value):
        self.__output = value


class SourceNode(AbstractNode):
    def __init__(self, extractor_core: ExtractorCore, flatten=False) -> TP.Return0:
        self.flatten = flatten
        super().__init__(extractor_core)

    def run(self):
        if self.flatten:
            for record in self.core:
                if isinstance(record, TP.Iterable):
                    # TODO: Give an option to raise error in case if no iterable returned
                    yield from record
        else:
            yield from self.core
            
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
        other.input = self.run()
        return other

    def __lshift__(self, other: AbstractNode):
        # TARGET: self
        raise NotImplementedError("SourceNode should not be linked into.")

    # RSH1: A >> B -> B
    # RSH2: B >> A -> A
    # LSH1: A << B -> A
    # LSH2: B << A -> B
    
class SinkNode(AbstractNode):
    def __init__(self, loader_core: LoaderCore) -> TP.Return0:
        super().__init__(loader_core)

    def run(self):
        for record in self.input:
            self.core.load(record)

        return True

    ############################################################
    # Node Operations
    ############################################################

    def __rshift__(self, other: AbstractNode):
        raise NotImplementedError("SinkNode should not be linked from.")

    def __lshift__(self, other: AbstractNode):
        # TARGET: self
        self.input = other.run()
        return self

    # RSH1: A >> B -> B
    # RSH2: B >> A -> A
    # LSH1: A << B -> A
    # LSH2: B << A -> B



class ProcessNode(AbstractNode):
    def __init__(self, processor_core: ProcessorCore) -> TP.Return0:
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


class JunctionNode(AbstractNode): ...
# TODO: FINISH


class FunnelNode(AbstractNode):
    ...
# TODO: FINISH