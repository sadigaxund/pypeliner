from .abtract import AbstractCore
from ...types.custom import *
from ...types import *


class JunctionCore(AbstractCore):
    @AbstractMethod
    def segregate(record: Whatever) -> Whatever: ...
