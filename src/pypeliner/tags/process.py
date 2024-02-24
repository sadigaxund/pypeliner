def processor(func):
    func.udf_processor = True
    return func

# TODO: Maybe add tag which differentiates processors:
# one that transforms data, and
# another that just takes it's value and executes process with it
