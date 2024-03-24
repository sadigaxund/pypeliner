# UTILS
from .abtract import AbstractCore

# TYPING
from abc import abstractmethod as AbstractMethod
from typing import Any


class FunnelCore(AbstractCore):
    @AbstractMethod
    def aggregate(*records: Any) -> Any: ...
    