from pypeliner.nodes import *
from pypeliner import maker

from example import extract as e1
from example import extract_input as e2
from example import transform as t
from example import load as l

ecore1 = maker.create_extractor_core(e1)
ecore2 = maker.create_extractor_core(e2)
tcore = maker.create_processor_core(t)
lcore = maker.create_loader_core(l)

source_node_one = SourceNode(ecore1)
source_node_two = SourceNode(ecore2)
transf_node = ProcessNode(tcore)
load_node = SinkNode(lcore)

pipeline = source_node_one >> source_node_two >> transf_node >> load_node

pipeline.run()

# for el in source_node_two.run():
#     print(el)


