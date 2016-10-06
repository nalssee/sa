from pydwork.sqlplus import *
from pydwork.util import yyyymmdd, pmap
import statistics as st


set_workspace('workspace')


# difference of opinion
# i.e., standard deviation
def diffop(rs):
    if len(rs) < 2:
        return ''
    # stdev of net negative ratios
    return st.stdev((r.nneg - r.npos) / r.ntot for r in rs)


# articles_prets needed
with dbopen('space.db') as c:

    # c.save(reel('articles_prets'), name='articles_prets')
    # c.show('select count(*) from articles_prets')
    def datasetB():

        def func(rs):
            try:
                # main article
                # rarely, but a few articles do not have main.
                mrow = next(x for x in rs if x.type == 'main')
            except:
                return

            mdate = mrow.date
            mrow.yyyymm = str(mdate)[0:6]

            # rows within 2 days, t, t + 1
            mdate1 = yyyymmdd(mdate, 1)
            rs1 = [r for r in rs if r.date == mdate or r.date == mdate1]
            rs2 = [r for r in rs if r.ntot > 100 and \
                   (r.date == mdate or r.date == mdate1)]

            mrow.diffop1 = diffop(rs1)
            mrow.diffop2 = diffop(rs2)

            mrow.rpos = mrow.npos / mrow.ntot
            mrow.rneg = mrow.nneg / mrow.ntot
            mrow.rnneg = (mrow.nneg - mrow.npos) / mrow.ntot

            return mrow

        for rs in c.reel(
            """
            select * from articles_prets
            where ntot > 0
            order by id_article

            """, group='id_article'):
            r = func(rs)
            if r is not None:
                yield r


    # c.drop('datasetB')
    c.save(datasetB)
    c.show('datasetB')
    # c.show('select count(*) from datasetB')



