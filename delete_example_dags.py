from datetime import datetime, timedelta, time

t =datetime.now().time()
print(t)
print(time(11, 30))
print(time(12, 0))
if time(11, 30) <= t <= time(12, 0):
    print(" Skipping (outside 9:00–11:30)")
else:
    print('running task')
