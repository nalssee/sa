from pydwork.sqlplus import *


set_workspace('workspace')


with dbopen('space1.db') as c1:
    c1.show('daily')
