from example import etl
from pypeliner.nodes.ingress import IngressNode
from pypeliner.nodes.process import ProcessorNode
from pypeliner.nodes.egress import EgressNode
from pypeliner.maker import *

gen = create_extractor_core(etl)
mod = create_processor_core(etl)
lod = create_loader_core(etl)

pipeline = IngressNode(gen) >> ProcessorNode(mod) >> EgressNode(lod)
pipeline.run()