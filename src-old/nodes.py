


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
        self.output = (self.core.merge(*flow) for flow in conjoined_flows)
        return self

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
        try:
            self.output
        except NotImplementedError:
            self.run()
        finally:
            other.input = self.output
        return other
    
    def __lshift__(self, other: AbstractNode):
        try:
            other.output
        except NotImplementedError:
            other.run()
        finally:
            self.input_flows.append(other.output)
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
            return self

        def __next__(self):
            return self.output.next(self.i)
            
    
    def __init__(self, junction_core: JunctionCore, outflows = 2, input = None) -> TP.Void:
        self.outflows = outflows
        self.input = input
        super().__init__(junction_core)
        
        
    def __getitem__(self, key):
        try:
            self.output
        except NotImplementedError:
            self.run()
        finally:
            return JunctionNode.OutputSelector(self.output, key)

    def run(self):
        if isinstance(self.input, TP.Iterable):
            self.output = (self.core.divide(el) for el in self.input)
            self.output = JunctionNode.OutputGenerator(self.output, self.outflows)
            self.output_pointer = 0
        return self
    
    def get_next(self):
        try:
            self.output
        except NotImplementedError:
            self.run()
        finally:
            self.output_pointer += 1
            return self[self.output_pointer - 1]
                    
    def __rshift__(self, other: AbstractNode):
        other.input = self.get_next()
        return other
    
    def __lshift__(self, other: AbstractNode):
        try:
            other.output
        except NotImplementedError:
            other.run()
        finally:
            self.input = other.output
        return self

