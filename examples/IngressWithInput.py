from src2.cores import IngressCore
from src2.types.custom import Bool, Never, Whatever


class Ingress(IngressCore, Type=IngressCore.Types.INPUT):
    def constructor(self):
        print("start")

    def destructor(self, exc_type, exc_value, traceback):
        print("done")
        
    def process_input(self, new_input):
        self.input = new_input

    def produce(self) -> Whatever:
        yield next(self.input) + 1

with Ingress(flatten=True, input=[1, 2, 3]) as ingress:
    for record in ingress:
        print(record)
