def loader(func):
    func.udf_loader = True
    return func
