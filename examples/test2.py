
from typing import Any, Iterable
from src.cores import *
from src.nodes import *

class Test(IngressCore):
    def destructor(self, exc_type, exc_value, traceback):
        return super().destructor(exc_type, exc_value, traceback)
    def constructor(self):
        return super().constructor()
    
    def produce(self) -> Iterable | Any:
        return super().produce()