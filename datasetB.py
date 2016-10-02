from pydwork.sqlplus import *
from pydwork.util import yyyymmdd
import statistics as st

set_workspace('workspace')

def diffop(rs):
    if len(rs) < 2:
        return ''
    # stdev of net negative ratios
    return st.stdev((r.nneg - r.npos) / r.ntot for r in rs)

with dbopen('space.db') as c:
    # c.drop('articles_prets')
    c.save(reel('articles_prets'), name='articles_prets')
    # c.show('select count(*) from articles_prets')
    def datasetB():
        for rs in c.reel(
            """
            select * from articles_prets
            where ntot > 0
            order by id_article
            """, group='id_article'):
            try:
                # main article
                mrow = next(x for x in rs if x.type == 'main')
            except:
                continue

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

            yield mrow

    # c.save(datasetB, overwrite=True)
    c.show('select count(*) from datasetB')










