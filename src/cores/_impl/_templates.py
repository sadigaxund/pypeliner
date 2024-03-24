def void_callback(*args, **kwargs): ...


def void_execute(self):
    raise StopIteration("Ingress Core has finished.")


def void_available(self):
    return True


def void_iterate(self):
    ...


def void_input_setter(self, input):
    return input


def no_change(x): 
    return x
