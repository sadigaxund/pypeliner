
from .Types import *
import time

def empty():
    # does nothing
    ...

class SinkNode:
    
    # LOADER 
    def __loader__(self, value: Any) -> Any:
        raise NotImplementedError("Functionality to load record was not implemented!")

    @property
    def loader(self) -> Callable[[DefaultOutputType], Optional[bool]]:
        # TODO: Ensure self
        return self.__loader__

    @loader.setter
    def loader(self, new_loader: Callable) -> None:
        self.__loader__ = new_loader

    # OUTPUT
    def __output__(self):
        raise NoOutputAttributeWarning(self)
    
    # INPUT

    @property
    def input(self) -> DefaultOutputType:
        if not self.__input__:
            raise InputNotLinkedError
        else:
            return self.__input__

    @input.setter
    def input(self, value: DefaultInputType):
        if not isinstance(value, DefaultInputType):
            raise ValueError(
                f"Input has to be either of type Generator, Iterator or Collection. Recieved: {type(value)}"
            )

        def create_input_function():
            yield from value

        self.__input__ = create_input_function()
        
    # MAIN LOGIC
    # Options:
    #     1. Pass individual loader function(as udf) and apply over all records
    #     2. Pass mass loader function that has implementation to load all records
    #     3. Use Custom loader function: KafkaLoader
    
        
    def __init__(self, udf_loader, input=None) -> None:
        self.__input__ = None
        self.future_value = None
        self.loader = udf_loader
        self.__counter__ = 0
        if input is not None:
            self.input = input
    
    SENTINEL = 0x73656E74696E656C
    
    def __next__(self):
        if self.future_value is not None:
            retval = self.future_value
            self.future_value = None
            return retval
        try:
            retval = next(self.input)
            return retval
        except StopIteration:
            return SinkNode.SENTINEL
    
    def more(self):
        # cases:
        # 1. if f_val not None or sentinel -> then use it
        # 2. if f_val is None -> we don't know, go fetch
        # 3. if f_val is SENTINEL -> gameover
        match self.future_value:
            case None:
                # means we could still have something
                self.future_value = self.__next__()
                return self.future_value != SinkNode.SENTINEL

            case SinkNode.SENTINEL:
                # means it already ended
                return False
                ...
            case _:
                # means we already checked before, and there is at least f_val
                return True
            
    def __load_bulk(self):        
        try:
            # Started Loading...
            while True:
                value = yield  # get value from as you go
                self.loader(value)
        finally:
            # Finished Loading...
            ...
    
    def start(self):
        self.ETL = self.__load_bulk()
        self.ETL.send(None) # Generator (for loader) started
    
    def close(self):
        self.ETL.close()  # Generator (for loader) stopped
        
    def step(self) -> bool:
        record = self.__next__()
        
        if record == SinkNode.SENTINEL:
            return True # pipeline done
        
        self.ETL.send(record)
        return False
    
    def __back_pressure__(self, chunk_size, backoff_seconds):
        self.__counter__ += 1
        
        if self.__counter__ >= chunk_size:
            time.sleep(backoff_seconds)
            self.__counter__ = 0
    
    
    def run(self, 
            on_start: Callable = empty,
            on_exit: Callable = empty,
            after_each_load: Callable = empty,
            before_each_load: Callable = empty,
            on_fallback: Callable = empty,
            blacklist: List = None,
            whitelist: List = None,
            backoff_seconds: int = 0,
            chunk_size: int = 500) -> None:
        self.start()
        on_start()
        while self.more():
            before_each_load()
            try:
                self.step()
            except Exception as e:
                # TODO: Refactor so that, it would pass record as a parameter
                on_fallback()
            finally:
                after_each_load()
                self.__back_pressure__(chunk_size, backoff_seconds)
        on_exit()
        self.close()
        
    
        
    
        
        

