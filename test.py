# from src2.cores import IngressCore
# from src2.types.custom import Bool, Never, Whatever
# import time

# class Ingress(IngressCore, Type=IngressCore.Types.FUNCTION):
#     def constructor(self):
#         from collections import deque
#         self.values = deque([1, 2, 3, 4, 5])
#         self.cursor = None
#         print("start")

#     def destructor(self, exc_type, exc_value, traceback):
#         print("done")

#     def available(self) -> bool:
#         return len(self.values) > 0
    
#     def iterate(self) -> Never:
#         self.cursor = self.values.popleft()
    
#     def produce(self) -> Whatever:
#         for e in range(0, self.cursor):
#             yield e
            
# with Ingress(flatten=True) as ingress:
#     for record in ingress:
#         print(record)
import itertools

for r in itertools.chain([1,2,3]):
    print(r)