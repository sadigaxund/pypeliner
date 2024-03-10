from .abtract import AbstractCore
from ...types.custom import *
from ...types import *


class FunnelCore(AbstractCore):
    @classmethod
    @AbstractMethod
    def aggregate(*values: Whatever) -> Whatever: ...