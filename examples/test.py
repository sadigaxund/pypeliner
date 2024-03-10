from src.cores._core_implementations.process import Core as ParentCore
class Core(ParentCore): 
    
    @ParentCore.step
    def test1(record):
        print("test 1")
        return record + 2

    @ParentCore.step
    def test3(record):
        print("test 2")
        return record + 1

    @ParentCore.step
    def test2(record):
        print("test 3")
        return record + 2

    @ParentCore.step
    def test5(record):
        print("test 4")
        return record + 3

class Core2(ParentCore): 
    
    @ParentCore.step
    def test1(record):
        print("test 1")
        return record + 2

    @ParentCore.step
    def test3(record):
        print("test 2")
        return record + 1

    @ParentCore.step
    def test2(record):
        print("test 3")
        return record + 2

    @ParentCore.step
    def test5(record):
        print("test 4")
        return record + 3
 
print(Core.process(record=1))
print(Core2.process(record=2))