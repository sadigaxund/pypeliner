from pypeliner.tags.extract import execute, iterate, available

mylist = list(range(10))
index = 0


@execute
def run():
    return mylist[index]

@available
def has_next():
    return index < len(mylist) - 1

@iterate
def iterate():
    global index
    index += 1
