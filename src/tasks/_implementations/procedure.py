from ._templates import DoesNothing

from typing import Any, Self

class Procedure:
    ThatDoesNothing: Self = None

    def __init__(self, callback) -> None:
        self.callback = callback

    def execute(self, *args, **kwargs) -> Any:
        try:
            return self.callback(*args, **kwargs)
        except Exception as e:
            e.__traceback__ = None
            raise

    def __call__(self, *args, **kwargs) -> Any:
        return self.execute(*args, **kwargs)
        
        
Procedure.ThatDoesNothing = Procedure(DoesNothing)