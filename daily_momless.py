"""
subtract momentum factor from daily return
"""

from pydwork.sqlplus import *
from pydwork.npc import npc
import statsmodels.api as sm

set_workspace('workspace')


with dbopen('space1.db') as c1:
    c1.save(reel('daily'), name='daily')

    c1.show('select count(*) from daily')
    c1.show('daily')



