from typing import Any
from .base import BasePolicy



class Idempotent(BasePolicy):
    def execute(self, *args, **kwargs) -> Any:
        return super().execute(*args, **kwargs)