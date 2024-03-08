from .cores import *

###################
# HELPERS #########
###################

def get_function(module, attribute):
    for func_name in dir(module):
        func = getattr(module, func_name)
        if hasattr(func, attribute):
            yield func
    yield None

def extract_extractor_udf_from_module(module):
    execute = next(get_function(module, 'udf_execute'))
    iterate = next(get_function(module, 'udf_iterate'))
    available = next(get_function(module, 'udf_available'))
    input_handler = next(get_function(module, 'udf_input'))
    return execute, iterate, available, input_handler

def extract_process_udf_from_module(module):
    processors = get_function(module, 'udf_processor')
    processors = list(processors)
    processors.pop(len(processors) - 1)
    return processors

def extract_load_udf_from_module(module):
    loaders = get_function(module, 'udf_loader')
    loaders = list(loaders)
    loaders.pop(len(loaders) - 1)
    return loaders

###################
# MAINS ###########
###################
def create_extractor_core(module, input = None):
    module.input = input
    funcs = extract_extractor_udf_from_module(module)
    return ExtractorCore(funcs[0], funcs[1], funcs[2], funcs[3], input)


def create_processor_core(module):
    funcs = extract_process_udf_from_module(module)
    return ProcessorCore(*funcs,
                         immutable=module.immutable,
                         forgiving=module.forgiving,
                         fallback=module.fallback,
                         raise_error=module.raise_error)
    
def create_loader_core(module):
    funcs = extract_load_udf_from_module(module)
    return LoaderCore(*funcs)

def create_junction_core(divider):
    return JunctionCore(divider)

def create_funnel_core(merger):
    return FunnelCore(merger)
