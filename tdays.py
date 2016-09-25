# Trading days(daily)

from pydwork.sqlplus import *

set_workspace('/Users/kenjin/pw/sa/workspace')

dates = set()
for i, r in enumerate(reel('daily')):
    dates.add(r.date)
    if i % 100000 == 0:
        print(i)

dates = list(dates)
dates.sort()

rows = []
for i, date in enumerate(dates):
    r = Row()
    r.date = date
    r.datei = i
    rows.append(r)

Rows(rows).show(filename='tdays', n=None)

with dbopen('space.db') as c:
    c.save(reel('tdays'), name='tdays', overwrite=True)
    c.show('tdays')

print('done, check tdays.csv in workspace')


