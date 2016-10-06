from pydwork.sqlplus import *
from pydwork.util import isnum, pmap

set_workspace('workspace')

with dbopen('space.db') as c:
    c.show('articles_prets')
