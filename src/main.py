from example import etl
from pypeliner.nodes import IngressNode
from pypeliner.nodes import ProcessNode
from pypeliner.nodes import EgressNode
from pypeliner import maker

gen = maker.create_extractor_core(etl)
mod = maker.create_processor_core(etl)
lod = maker.create_loader_core(etl)

pipeline = IngressNode(gen) >> ProcessNode(mod) >> EgressNode(lod)
pipeline.run()


