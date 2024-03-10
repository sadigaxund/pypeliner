# from types.custom import Whatever
from src.cores.implement.egress import Core
from src.types.custom import Whatever
import time
class Loader(Core):
    
    def consume(self, record: Whatever) -> Whatever:
        if record == 5:
            raise Exception("Fuck Yeah")
    
    def callback(self, record, exception: Exception):
        if exception:
            print(f"Load Failed for Record=[{record}] with Exception=[{exception}]")
        else:
            print(f"Successfully Loaded Record=[{record}]")
    def pulse(self):
        time.sleep(1)
        
    def destructor(self, exc_type, exc_value, traceback):
        print("Destruct")
        return super().destructor(exc_type, exc_value, traceback)
    
    def constructor(self):
        print("Construct")
        return super().constructor()
    

with Loader(input=range(10), heartbeat=2) as loader:
    loader.run()
