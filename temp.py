from pydwork.sqlplus import *
from pydwork.util import isnum, pmap

set_workspace('workspace')

with dbopen('space.db') as c:
#    c.show(
#        """
#        select avg(ret
#        """
#    )
    c.show(
        """
        select avg(ret_0),
        avg(ret_less2_p1), avg(ret_less4_p1), avg(ret_p1),
        avg(ret_less2_p2), avg(ret_less4_p2), avg(ret_p2),
        avg(ret_less2_p3), avg(ret_less4_p3), avg(ret_p3),
        avg(ret_less2_p250), avg(ret_less4_p250), avg(ret_p250)



        from articles_prets
        """)

#with dbopen('space1.db') as c1:
#    c1.show(
#        """
#        select avg(ret),
#        """
#    )
