def define_types():
    from . import custom
    
    global CoreInputType, CoreOutputType, NodeInputType, NodeOutputType, InputNotLinkedError, OutputNotLinkedError
    
    CoreInputType = custom.Iterable[custom.Whatever]
    CoreOutputType = custom.Generator[custom.Whatever, None, None]

    NodeInputType = custom.Iterable[custom.Whatever]
    NodeOutputType = custom.Generator[custom.Whatever, None, None]

    InputNotLinkedError = NotImplementedError("Input was not linked!")
    OutputNotLinkedError = NotImplementedError("Output was not linked!")

    def NoInputAttributeWarning(cls):
        return UserWarning(f'{cls.__class__.__name__} does not take input.')

    def NoOutputAttributeWarning(cls):
        return UserWarning(f'{cls.__class__.__name__} does not produce output.')


if '.types' in __name__:
    define_types()