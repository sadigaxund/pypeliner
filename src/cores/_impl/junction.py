# UTILS
from .abtract import AbstractCore

# TYPING
from abc import abstractmethod as AbstractMethod
from typing import Any, Tuple

class JunctionCore(AbstractCore):
    @AbstractMethod
    def segregate(record: Any) -> Tuple[Any]: ...
