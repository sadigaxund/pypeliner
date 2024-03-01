def divisor(func):
    func.udf_loader = True
    return func


def merger(func):
    func.udf_merger = True
    return func
