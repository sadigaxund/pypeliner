def __get_internal_types():
    import src2.types.custom as internals
    return internals

def define_types():
    internals = __get_internal_types()
    
    global CoreInputType, CoreOutputType, NodeInputType, NodeOutputType, InputNotLinkedError, OutputNotLinkedError
    
    CoreInputType = internals.Iterable[internals.Whatever]
    CoreOutputType = internals.Generator[internals.Whatever, None, None]

    NodeInputType = internals.Iterable[internals.Whatever]
    NodeOutputType = internals.Generator[internals.Whatever, None, None]

    InputNotLinkedError = NotImplementedError("Input was not linked!")
    OutputNotLinkedError = NotImplementedError("Output was not linked!")

    

    def NoInputAttributeWarning(cls):
        return UserWarning(f'{cls.__class__.__name__} does not take input.')


    def NoOutputAttributeWarning(cls):
        return UserWarning(f'{cls.__class__.__name__} does not produce output.')


if '.types' in __name__:
    define_types()