from pydwork.sqlplus import *
import statistics as st
# import statsmodels.formula.api as sm


from helpers import *

set_workspace('/Users/kenjin/pw/sa/workspace')


def main():
    with dbopen('space.db') as c:

        c.save(reel('ff6.csv'), name='ff6', overwrite=True)

        load_mret(c)

        c.drop('mret1')

        mret1 = """
        create table if not exists mret1 as
        select a.*, b.ret as ret_p1, abs(a.prc * a.shrout) as size,
        abs(b.prc * b.shrout) as size_p1
        from mret a
        left join mret b
        on a.tsymbol = b.tsymbol and a.yyyymm = b.yyyymm_n1
        """
        c.run(mret1)

        def avg_words():
            """
            get avg words ratio from articles_cnt
            """
            for rs in c.reel(
                # only those with ntot > 0
                """
                select *, substr(date, 1, 6) as yyyymm from articles_cnt
                where ntot > 0
                order by yyyymm, ticker
                """, group='yyyymm, ticker'
            ):
                r = Row()

                r.tsymbol = rs[0].ticker
                r.yyyymm = rs[0].yyyymm
                r.yyyymm_n1 = yyyymm(rs[0].yyyymm, 1)

                # main articles only
                rs_m = [r for r in rs if r.type == 'main']
                r.n = len(rs_m)
                r.avgntot  = mean1([r.ntot for r in rs_m])
                # it's better to read in percentage, just that.
                r.avgrpos  = mean1([r.npos / r.ntot for r in rs_m]) * 100
                r.avgrneg  = mean1([r.nneg / r.ntot for r in rs_m]) * 100
                r.avgrnneg = mean1([(r.nneg - r.npos) / r.ntot
                                    for r in rs_m]) * 100

                # comments only
                rs_c = [r for r in rs if r.type == 'comment']
                r.n_c = len(rs_c)
                r.avgntot_c  = mean1([r.ntot for r in rs_c])

                r.avgrpos_c  = mean1([r.npos / r.ntot for r in rs_c]) * 100
                r.avgrneg_c  = mean1([r.nneg / r.ntot for r in rs_c]) * 100
                r.avgrnneg_c = mean1([(r.nneg - r.npos) / r.ntot
                                      for r in rs_c]) * 100
                yield r

        c.drop('avg_words')
        c.save(avg_words)


        dataset = """
        create table if not exists dataset as
        select
        a.permno, a.cusip, a.tsymbol, a.ticker, a.shrcd, a.exchcd, a.shrcls,
        a.yyyymm, a.yyyy,
        a.prc, a.shrout, a.ret, a.ret_p1, a.size, a.size_p1,

        b.n, b.avgntot, b.avgrpos, b.avgrneg, b.avgrnneg,
        b.n_c, b.avgntot_c, b.avgrpos_c, b.avgrneg_c, b.avgrnneg_c,

        c.mkt_rf, c.smbf, c.hml, c.rmw, c.cma, c.mom, c.rf, c.smb

        from mret1 as a
        left join avg_words as b
        on a.tsymbol = b.tsymbol and a.yyyymm = b.yyyymm_n1

        left join ff6 as c
        on a.yyyymm = c.yyyymm
        """

        c.drop('dataset')
        c.run(dataset)
        print('DATASET!!!!')
        c.show('dataset')

# onetime
def load_mret(c):
    def mret():
        for r in reel('mret.csv'):
            # There isn't a single pair of duplicates with non-empty 'prc's
            # as far as tsymbol is concerned, which is not the case for ticker
            # So use tsymbol instead of tikcer
            if r.tsymbol != '' and r.prc != '' and \
               r.ret != 'C' and r.ret != '':
                # set it to percentage return.
                # I don't know if it is a good idea
                r.ret = float(r.ret) * 100
                r.cusip = 'A' + r.cusip
                r.permno = 'A' + r.permno
                r.yyyymm = r.date[0:6]
                r.yyyy = r.date[0:4]
                r.yyyymm_n1 = yyyymm(r.yyyymm, 1)
                yield r

    c.drop('mret')
    c.save(mret(), name='mret')


def mean1(xs):
    if xs == []:
        return 0
    else:
        return st.mean(xs)


main()


