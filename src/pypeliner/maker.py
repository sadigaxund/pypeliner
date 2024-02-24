from .cores import *

###################
# HELPERS #########
###################

def get_function(module, attribute):
    functions = []
    
    for func_name in dir(module):
        func = getattr(module, func_name)
        if hasattr(func, attribute):
            functions.append(func)
    
    return functions

def extract_extractor_udf_from_module(module):
    execute = get_function(module, 'udf_execute')[0]
    iterate = get_function(module, 'udf_iterate')[0]
    available = get_function(module, 'udf_available')[0]
    return execute, iterate, available

def extract_process_udf_from_module(module):
    processors = get_function(module, 'udf_processor')
    return processors

def extract_load_udf_from_module(module):
    loaders = get_function(module, 'udf_loader')
    return loaders

###################
# MAINS ###########
###################
def create_extractor_core(module):
    funcs = extract_extractor_udf_from_module(module)
    return ExtractorCore(funcs[0], funcs[1], funcs[2])


def create_processor_core(module):
    funcs = extract_process_udf_from_module(module)
    return ProcessorCore(*funcs,
                         immutable=module.immutable,
                         forgiving=module.forgiving,
                         fallback=module.fallback)
    
def create_loader_core(module):
    funcs = extract_load_udf_from_module(module)
    return LoaderCore(*funcs)
