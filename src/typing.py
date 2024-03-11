# TYPING

from enum import Enum

def import_types(global_namespace):
    
    from abc import (
        ABC as AbstractClass,
        ABCMeta as AbstractMetadata,
        abstractmethod as AbstractMethod
    )
    
    global OneOf, Iterable, Whatever, Void, Tuple
    
    from typing import (
        Union as OneOf, 
        Iterable, 
        Any as Whatever,
        NoReturn as Void, 
        Tuple
    )
    
    global Enum, CoreInput
    CoreInput = OneOf[Iterable, Whatever]
    from enum import Enum
    
    global_namespace['AbstractClass'] = AbstractClass
    global_namespace['AbstractMetadata'] = AbstractMetadata
    global_namespace['AbstractMethod'] = AbstractMethod
    global_namespace['OneOf'] = OneOf
    global_namespace['Iterable'] = Iterable
    global_namespace['Whatever'] = Whatever
    global_namespace['Void'] = Void
    global_namespace['Tuple'] = Tuple
    global_namespace['CoreInput'] = CoreInput
    global_namespace['Enum'] = Enum
    