from tasker.structs import Task, Function
from tasker.policies import RetryPolicy, ErrorStrategy

@Task.create(name="Some Function", 
        description="Does absolutely nothing at all.", 
        retry_policy=RetryPolicy.Linear(retries=5, backoff_seconds=1),
        error_strategy=ErrorStrategy.NeverHandle())
def some(abc):
    print(f'{abc=}')
    raise ValueError("Some fucking Error")

some(123)

