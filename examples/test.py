# from types.custom import Whatever
from src.cores.implement.ingress import Core, Type

class Source(Core, Type=Type.INPUT):
    def constructor(self):
        return super().constructor()
    
    def destructor(self, exc_type, exc_value, traceback):
        return super().destructor(exc_type, exc_value, traceback)
    
    def produce(self):
        return next(self.input)
    
with Source(flatten=True, input=100) as src:
    for i in src:
        print(i)
    

