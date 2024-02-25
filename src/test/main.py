import sys
sys.path.append('../')

from pypeliner.nodes import *
from pypeliner import maker

# gen = maker.create_extractor_core(etl)
# mod = maker.create_processor_core(etl)
# lod = maker.create_loader_core(etl)

# pipeline = IngressNode(gen) >> ProcessNode(mod) >> EgressNode(lod)
# pipeline.run()

from priceline.listings import (
    extract_hotels_list as E1,
    extract_hotel_details as E2
)


C1 = maker.create_extractor_core(E1)
C2 = maker.create_processor_core(E2)


pipeline = IngressNode(C1, flatten=True) >> ProcessNode(C2)

for r in pipeline.run():
    print(r)
    break
