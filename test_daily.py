from pydwork.sqlplus import *
# this is just for testing purposes
from itertools import islice


set_workspace('workspace')


def dupcheck():
    def is_anydup(rs):
        rset = set()
        for r in rs:
            if r.date in rset:
                print(r.tsymbol)
                Rows(rs).show(n=1000000)
                raise
            else:
                rset.add(r.date)

    with dbopen('space1.db') as c1:
        cnt = 0
        for rs in c1.reel(
            """
            select * from daily
            where isnum(ret) and
            tsymbol != '' and
            (shrcd = 10 or shrcd = 11) and
            (exchcd = 1 or exchcd = 2 or exchcd = 3)
            order by tsymbol
            """, group='tsymbol'):
            is_anydup(rs)


# tests if trading days for NYSE, NASDAQ, and AMEX are all the same.
def trading_days_across_exchanges():
    with dbopen('space1.db') as c1:
        c1.write(
            """
            select * from daily
            where tsymbol='SIM' or tsymbol='MSFT' or tsymbol='IBM'
            order by tsymbol, date
            """,
            filename='MAJOR_FIRM_EACH_EXCHANGE')



# dupcheck()
# trading_days_across_exchanges()
