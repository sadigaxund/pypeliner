from example import extract
from example import transform
from pypeline.cores.maker import *

gen = create_extractor_core(extract)
mod = create_processor_core(transform)

for i in gen:
    print(mod.process(i))

