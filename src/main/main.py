from example import extract, transform, load
from pypeliner.nodes.ingress import IngressNode
from pypeliner.nodes.process import ProcessorNode
from pypeliner.nodes.egress import EgressNode
from pypeliner.maker import *

gen = create_extractor_core(extract)
mod = create_processor_core(transform)
lod = create_loader_core(load)


node = IngressNode(gen) >> ProcessorNode(mod) >> EgressNode(lod)

node.run()