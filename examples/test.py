



def handler_my_arse(a, b, c):
    print('||', a, '||', b, '||', c, '||')


from src.tasks._implementations.retry.timebased import TimeBased
from src.tasks._implementations.retry.eventbased import EventBased


flag = False
i = 0
def event():
    return flag

@EventBased(heartbeat=2, attempts=3, event=event)
def test(abc):
    global i, flag
    
    if i == 5:
        flag = True
    i += 1
    print(f"ABC={abc}")
    
    raise Exception(f"I hate: {abc}")




test.callback.event = event

print(test(123))