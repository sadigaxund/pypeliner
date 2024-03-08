def iterate(func):
    func.udf_iterate = True
    return func


def available(func):
    func.udf_available = True
    return func


def execute(func):
    func.udf_execute = True
    return func


def input(func):
    func.udf_input = True
    return func