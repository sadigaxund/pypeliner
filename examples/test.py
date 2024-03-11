from src.cores import *
from src.nodes import *
from itertools import tee
from typing import *
import time


class Extract(IngressCore, Type=IngressType.INPUT):
    def produce(self) -> Any:
        return next(self.input)
    def constructor(self):
        return super().constructor()
    def destructor(self, exc_type, exc_value, traceback):
        return super().destructor(exc_type, exc_value, traceback)

class Transform(ProcessCore):
    @processor
    def add1(record):
        return record + 1
    
    @processor
    def mult2(record):
        return record * 2

    @processor
    def mult2(record):
        return record * 2 
    
class Load(EgressCore):
    def __init__(self, input=None) -> None:
        super().__init__(input, heartbeat=3)
    
    def constructor(self):
        return super().constructor()
    
    def callback(self, record, exception: Exception):
        if exception == None:
            print("Loaded:", record)
        
    def destructor(self, exc_type, exc_value, traceback):
        return super().destructor(exc_type, exc_value, traceback)
    
    def pulse(self):
        print("Sleeping...")
        time.sleep(1)
        
    def consume(self, record: Any) -> Any:
        return super().consume(record)

class Sum(FunnelCore):
    def aggregate(*values):
        sum = 0
        for value in values:
            if isinstance(value, int):
                sum += value
        return sum

class Div(JunctionCore):
    def segregate(record):
        return record, record *-1, None
    

node1 = IngressNode(Extract(input=range(15))) >> ProcessNode(Transform)
node2 = node1 >> JunctionNode(Div, outflows=3)
node3 = FunnelNode(Sum) 
# node3 = node3 + node2[0] + node2[1] + 

with Load() as ecore:
    node = node2[0] >> EgressNode(ecore)
    node.run()
        