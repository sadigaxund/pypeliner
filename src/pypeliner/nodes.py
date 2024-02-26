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
    def __init__(self, core: AbstractCore) -> TP.Void:
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
    
    @input.setter
    def input(self, value):
        self.__input = value

    @property
    def output(self):
        if isinstance(self.__output, NotImplementedError):
            raise self.__output

        return self.__output

    @output.setter
    def output(self, value):
        self.__output = value


class SourceNode(AbstractNode):
    def __init__(self, extractor_core: ExtractorCore, flatten=False) -> TP.Void:
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
    def __init__(self, loader_core: LoaderCore) -> TP.Void:
        super().__init__(loader_core)

    def run(self):
        for record in self.input:
            self.core.load(record)

        return True

    ############################################################
    # Node Operations
    ############################################################

    # def __rshift__(self, other: AbstractNode):
    #     raise NotImplementedError("SinkNode should not be linked from.")

    # def __lshift__(self, other: AbstractNode):
    #     # TARGET: self
    #     self.input = other.run()
    #     return self

    # RSH1: A >> B -> B
    # RSH2: B >> A -> A
    # LSH1: A << B -> A
    # LSH2: B << A -> B



class ProcessNode(AbstractNode):
    def __init__(self, processor_core: ProcessorCore) -> TP.Void:
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

    # def __lshift__(self, other: AbstractNode):
    #     # TARGET: self
    #     self.input = other.run()
    #     return self


def conjoined_enumerate(generators) -> TP.Generator[TP.List, None, None]:
    while True:
        next_elements = []
        all_done = True
        for gen in generators:
            try:
                next_element = next(gen)
                next_elements.append(next_element)
                all_done = False
            except StopIteration:
                next_elements.append(None)
        if all_done or all(elem is None for elem in next_elements):
            break
        
        yield next_elements


class FunnelNode(AbstractNode): 
    def __init__(self, funnel_core: FunnelCore) -> TP.Void:
        self.input_flows = list()
        super().__init__(funnel_core)

    def run(self):
        conjoined_flows = conjoined_enumerate(self.input_flows)
        
        for flow in conjoined_flows:
            yield self.core.merge(*flow)

    ############################################################
    # Node Operations
    ############################################################

    def __add__(self, new_input: TP.Whatever):
        # if type(new_input) != TP.NodeInputType:
        #     raise ValueError(f"Value=[{new_input}] should be of Type=[{TP.NodeInputType}]")
        new_generator = (el for el in new_input)
        self.input_flows.append(new_generator)
        return self

    def __rshift__(self, other: AbstractNode):
        # GIVES output
        self.input_flows.append(other.run())
        return self


class JunctionNode(AbstractNode):
    
    class OutputGenerator:
        def __init__(self, source, n):
            self.source = source # generator of n tuples
            
            from collections import deque
            self.output = [deque() for _ in range(n)]

        def iterate(self):
            try:
                record = next(self.source)
                for i, el in enumerate(record):
                    self.output[i].append(el)
                return True
            except StopIteration:
                return False
                
        def next(self, i):
            if len(self.output[i]) == 0:
                if not self.iterate():
                    raise StopIteration("No More Records for the Branch#"+str(i))
            
            return self.output[i].popleft() # FIFO
    #  TODO Cool name "Branch"
    class OutputSelector:
        def __init__(self, output, i) -> None:
            self.output = output
            self.i = i
            pass
        
        def __iter__(self):
            self.output.iterate()
            return self

        def __next__(self):
            return self.output.next(self.i)
            
    
    def __init__(self, junction_core: JunctionCore, outflows = 2, input = None) -> TP.Void:
        self.core = junction_core
        self.outflows = outflows
        self.input = input
        
    def __getitem__(self, key):
        return JunctionNode.OutputSelector(self.output, key)

    def run(self):
        ...
                    
    def __rshift__(self, other: AbstractNode):
        other.input = self[self.output_pointer]
        self.output_pointer += 1
        return self
    
    @property
    def input(self):
        return self.__input
    
    @input.setter
    def input(self, value):
        self.__input = (self.core.divide(el) for el in value)
        self.output = JunctionNode.OutputGenerator(self.input, self.outflows)
        self.output_pointer = 0

    @property
    def output(self):
        if isinstance(self.__output, NotImplementedError):
            raise self.__output

        return self.__output

    @output.setter
    def output(self, value):
        self.__output = value
