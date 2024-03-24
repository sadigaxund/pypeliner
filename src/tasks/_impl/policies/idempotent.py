from typing import Any
from .base import BasePolicy

class IdempotentPolicy(BasePolicy):
    def execute(self, *args, **kwargs) -> Any:
        return super().execute(*args, **kwargs)