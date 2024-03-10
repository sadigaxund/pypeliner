from src.cores.implement.process import Core
class Core1(Core): 
    
    @Core.step
    def test1(record):
        print("test 1")
        return record + 2

    @Core.step
    def test3(record):
        print("test 2")
        return record + 1

    @Core.step
    def test2(record):
        print("test 3")
        return record + 2

    @Core.step
    def test5(record):
        print("test 4")
        return record + 3

class Core2(Core): 
    
    @Core.step
    def test1(record):
        print("test 1")
        return record + 2

    @Core.step
    def test3(record):
        print("test 2")
        return record + 1

    @Core.step
    def test2(record):
        print("test 3")
        return record + 2

    @Core.step
    def test5(record):
        print("test 4")
        return record + 3
 
print(Core1.process(record=1))
print(Core2.process(record=2))

