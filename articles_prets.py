"""
add previous returns(HPR) to articles_cnt
"""
from pydwork.sqlplus import *
from pydwork.util import mpairs, isnum, pmap
import math
import bisect
from itertools import islice

import statsmodels.api as sm


set_workspace('workspace')



# buy and hold return, not in percentage
# if it is not consecutive, then just return ''
# the reason why I pass col is because you may want to get
# market value weighted return or just raw return
def bhr(rs, start, end, col='ret'):
    if start < 0 or end < 0:
        return ''

    result = 1
    cnt = 0
    for r in islice(rs, start, end):
        ret = getattr(r, col)
        if isnum(ret):
            result *= 1 + ret
            cnt += 1
        else:
            return ''
    # geometric mean
    if cnt == end - start:
        return result ** (1 / (end - start)) - 1
    # not enough rows
    else:
        return ''


# add ret_less2 and ret_less4
# ret_less2: 모멘텀과 마켓 갖고 regression 돌린 나머지 찌꺼기
# ret_less4: 4 factor 제거한 찌꺼기
def ret_less(rs, beg, end):
    if beg < 0:
        return

    Y = []
    X = []
    rs1 = []
    for r in islice(rs, beg, end):
        if r.ret == '':
            continue
        Y.append(r.ret)
        X.append([r.mkt_rf, r.mom])
        rs1.append(r)

    # 250 is the half of the whole period, admittedly somewhat arbitrary
    if len(Y) < 251:
        return

    result = sm.OLS(Y, sm.add_constant(X)).fit()
    intercept = result.params[0]
    for resid, r in zip(result.resid, rs1):
        r.ret_less2 = intercept + resid


    Y = []
    X = []
    rs1 = []
    for r in islice(rs, beg, end):
        if r.ret == '':
            continue
        Y.append(r.ret)
        X.append([r.mkt_rf, r.smb, r.hml, r.mom])
        rs1.append(r)

    # 250 is the half of the whole period, admittedly somewhat arbitrary
    if len(Y) < 251:
        return

    result = sm.OLS(Y, sm.add_constant(X)).fit()
    intercept = result.params[0]
    for resid, r in zip(result.resid, rs1):
        r.ret_less4 = intercept + resid


def turnover(rs, start, end):
    if start < 0 or end < 0:
        return ''

    result = 0
    cnt = 0
    for r in islice(rs, start, end):
        if isnum(r.vol) and r.vol > 0 and isnum(r.shrout) and r.shrout > 0:
            result += r.vol / r.shrout
            cnt += 1
        else:
            return ''
    if cnt == end - start:
        return result / (end - start)
    else:
        return ''

def compute_logsize(rs, idx):
    r = rs[idx]
    try:
        return math.log10(abs(r.shrout * r.prc))
    except:
        return ''


# There mustn't be any duplicates in rs
def fillin(rs, tdays):
    # empty value: ''

    # make a dictionary
    cols = rs[0].columns

    rs_dict = {}
    for r in rs:
        rs_dict[r.date] = r

    result = []
    for tday in tdays:
        found = rs_dict.get(tday, False)
        if found:
            result.append(found)
        else:
            # if there's no data for the date, make a dummy for it
            r = Row()
            for c in cols:
                # fill with empty string
                setattr(r, c, '')
            result.append(r)
    # now you have full set of rows
    return result


# you need two databases since you have to work on both at the same time
with dbopen('space.db') as c, dbopen('space1.db') as c1:
    c1.save(reel('ff4d'), name='ff4d')
    def daily():
        for r in reel('daily'):
            r.permno = 'A' + r.permno
            r.ncusip = 'A' + r.ncusip
            r.cusip = 'A' + r.cusip
            yield r

    # c1.drop('daily')
    c1.save(daily)
    # print('daily saved')
    # c1.drop('daily1')
    c1.run(
        """
        create table if not exists daily1 as
        select a.*,
        b.mkt_rf / 100 as mkt_rf,
        b.smb / 100 as smb,
        b.hml / 100 as hml,
        b.mom / 100 as mom,
        b.rf / 100 as rf

        from daily as a
        left join ff4d as b
        on a.date = b.date

        """)

    c.save(reel('articles_cnt'), name='articles_cnt')

    firms1 = c.reel(
    """
        select * from articles_cnt
        where
        date <= 20151231

        order by ticker, id_article, id_comment
    """, group='ticker')

    firms2 = c1.reel(
    """
        select * from daily1
        where tsymbol != '' and
        (exchcd = 1 or exchcd = 2 or exchcd = 3) and
        (shrcd = 10 or shrcd = 11) and
        isnum(ret) and ret > -1

        order by tsymbol, date
    """, group='tsymbol')



    c.save(reel('tdays'), name='tdays')
    tdays = [r.date for r in c.reel('select * from tdays order by date')]

    def articles_prets():
        def func(rs_tuple):
            rs1, rs2 = rs_tuple
            print(rs2[0].tsymbol)
            rs2 = fillin(rs2, tdays)

            # default values for rs2
            for r in rs2:
                r.ret_less2 = ''
                r.ret_less4 = ''

            result = []
            for ars in Rows(rs1).group('id_article'):
                # very rarely, but some articles do not have main,
                # I have no idea where it came from

                try:
                    main_article = next(x for x in ars if x.type == 'main')
                except:
                    continue

                main_date = main_article.date

                idx = bisect.bisect_left(tdays, main_date)
                ret_0 = bhr(rs2, idx, idx + 1)
                logsize = compute_logsize(rs2, idx)

                periods = [1, 2, 3, 5, 10, 20, 60, 125, 250]

                # add ret_less2, ret_less4
                ret_less(rs2, idx - 250, idx + 251)

                rets_prev = [bhr(rs2, idx - x, idx) for x in periods]
                rets_next = [bhr(rs2, idx + 1, idx + 1 + x) for x in periods]

                rets_less2_prev = [bhr(rs2, idx - x, idx, 'ret_less2')
                                   for x in periods]
                rets_less2_next = [bhr(rs2, idx + 1, idx + 1 + x, 'ret_less2')
                                   for x in periods]

                rets_less4_prev = [bhr(rs2, idx - x, idx, 'ret_less4')
                                   for x in periods]
                rets_less4_next = [bhr(rs2, idx + 1, idx + 1 + x, 'ret_less4')
                                   for x in periods]

                vwrets_prev = [bhr(rs2, idx - x, idx, 'vwretd') for x in periods]
                vwrets_next = [bhr(rs2, idx + 1, idx + 1 + x, 'vwretd') for x in periods]

                tnover_prev = [turnover(rs2, idx - x, idx) for x in periods]
                tnover_next = [turnover(rs2, idx + 1, idx + 1 + x) for x in periods]

                for r1 in ars:
                    r1.ret_0 = ret_0
                    r1.logsize = logsize

                    for prd, x in zip(periods, rets_less2_prev):
                        setattr(r1, 'ret_less2_p' + str(prd), x)
                    for prd, x in zip(periods, rets_less2_next):
                        setattr(r1, 'ret_less2_n' + str(prd), x)

                    for prd, x in zip(periods, rets_less4_prev):
                        setattr(r1, 'ret_less4_p' + str(prd), x)
                    for prd, x in zip(periods, rets_less4_next):
                        setattr(r1, 'ret_less4_n' + str(prd), x)


                    for prd, x in zip(periods, rets_prev):
                        setattr(r1, 'ret_p' + str(prd), x)
                    for prd, x in zip(periods, rets_next):
                        setattr(r1, 'ret_n' + str(prd), x)

                    for prd, x in zip(periods, vwrets_prev):
                        setattr(r1, 'vwret_p' + str(prd), x)
                    for prd, x in zip(periods, vwrets_next):
                        setattr(r1, 'vwret_n' + str(prd), x)

                    for prd, x in zip(periods, tnover_prev):
                        setattr(r1, 'tnover_p' + str(prd), x)
                    for prd, x in zip(periods, tnover_next):
                        setattr(r1, 'tnover_n' + str(prd), x)

                    result.append(r1)

            return result


        for rs in pmap(func, mpairs(firms1, firms2,
                                    lambda rs: rs[0].ticker,
                                    lambda rs: rs[0].tsymbol),
                       nworkers=3,
                       chunksize=3):
            yield from rs

    # c.drop('articles_prets')
    c.save(articles_prets)
    # c.write('articles_prets', filename='articles_prets')
    c.show('articles_prets')
