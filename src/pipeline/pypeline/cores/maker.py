from .extractor import ExtractorCore
from .processor import ProcessorCore

###################
# EXTRACT #######
###################

def execute(func):
    func.udf_execute = True
    return func

def iterate(func):
    func.udf_iterate = True
    return func

def available(func):
    func.udf_available = True
    return func

def extract_extractor_udf_from_module(module):
    def get_function(module, attribute):
        for func_name in dir(module):
            func = getattr(module, func_name)
            if hasattr(func, attribute):
                return func

    execute = get_function(module, 'udf_execute')
    iterate = get_function(module, 'udf_iterate')
    available = get_function(module, 'udf_available')
    return execute, iterate, available

def create_extractor_core(module):
    funcs = extract_extractor_udf_from_module(module)
    return ExtractorCore(funcs[0], funcs[1], funcs[2])
    
################### 
# TRANSFORM #######
###################

def processor(func):
    func.udf_processor = True
    return func


def extract_process_udf_from_module(module):
    def get_function(module, attribute):
        for func_name in dir(module):
            func = getattr(module, func_name)
            if hasattr(func, attribute):
                yield func

    processors = get_function(module, 'udf_processor')
    return list(processors)


def create_processor_core(module):
    funcs = extract_process_udf_from_module(module)
    return ProcessorCore(*funcs,
                         immutable=module.immutable,
                         forgiving=module.forgiving,
                         fallback=module.fallback)
