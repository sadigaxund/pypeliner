from .abtract import AbstractCore
from ...types.custom import *
from ...types import *
from ._functools import CoreNamespace, MultiFunction


class ProcessMetaCore(AbstractMetadata):
    def __init__(cls, name, bases, clsdict):
        super().__init__(name, bases, clsdict)
    
    def __new__(cls, name, bases, dct):
        
        # Skip below manipulations on parent class
        if AbstractCore in bases:
            return super().__new__(cls, name, bases, dct)
        
        # Merge all processor methods into single object
        accumulative = None
        for key in dct.copy():
            attr = dct[key]
            # Get those methods that are marked as processor
            if accumulative == None and isinstance(attr, MultiFunction):
                accumulative = attr
                
            # Clean up the rest of the class namespace
            if key not in ['__module__', '__qualname__', '__class__']:
                del dct[key]
                
        # Add newly formed accumulative processor as attribute
        dct['process'] = accumulative
        return super().__new__(cls, name, bases, dct)
    
class ProcessCore(AbstractCore, metaclass=ProcessMetaCore):
    
    namespaces = {}
    
    @staticmethod
    def step(func):
        module_name = func.__qualname__.split('.')[0]
        if not module_name in ProcessCore.namespaces:
            ProcessCore.namespaces[module_name] = CoreNamespace()

        return ProcessCore.namespaces[module_name].register(func)