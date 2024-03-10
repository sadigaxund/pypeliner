from .abtract import AbstractCore
from ...types.custom import *
from ...types import *


class JunctionCore(AbstractCore):
    @classmethod
    @AbstractMethod
    def segregate(*values: Whatever) -> Whatever: ...
