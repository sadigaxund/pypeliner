




def abstract_imports():
    from ._core_implementations.abtract import AbstractCore
    from ._core_implementations.ingress import IngressCore
    from ._core_implementations.process import ProcessCore
    from ._core_implementations.egress import EgressCore
    from ._core_implementations.junction import JunctionCore
    from ._core_implementations.funnel import FunnelCore
    
    
    from ._core_implementations._functools import Implementation
    
    implement = Implementation()
    implement.abtract = Implementation()
    implement.ingress = Implementation()
    implement.process = Implementation()
    implement.egress = Implementation()
    implement.junction = Implementation()
    implement.funnel = Implementation()
    
    