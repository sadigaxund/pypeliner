
from priceline.listings import extract_hotel_details as extract_details
from priceline.listings import extract_hotels_list as extract_hotels
from pypeliner import maker
from pypeliner.cores import *
from pypeliner.nodes import *


ecore1 = maker.create_extractor_core(extract_hotels)
# ecore2 = maker.create_extractor_core(extract_details)

enode1 = SourceNode(ecore1, flatten=True, format=JSON)
# enode2 = SourceNode(ecore2, flatten=True)

for record in enode1.run():
    print(record['hotelid_ppn'])
