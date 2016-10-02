from pydwork.sqlplus import *
from pydwork.util import isnum

import statsmodels.api as sm

set_workspace('workspace')


def star(val, pval):
    "put stars according to p-value"
    if pval < 0.001:
        return str(round(val, 4)) + '***'
    elif pval < 0.01:
        return str(round(val, 4)) + '**'
    elif pval < 0.05:
        return str(round(val, 4)) + '*'
    else:
        return str(round(val, 4))


with dbopen('space.db') as c:
    # c.write('datasetB', filename='datasetB')

    rs = Rows(c.reel(
        """
        select * from datasetB
        where
        date <= 20151231
        """))

    for yvar in ['rpos', 'rneg', 'rnneg']:
        print('y var', yvar)
        print(',intercept,logsize,avg daily(mkt adj)')
        for period in [1, 2, 3, 5, 10, 20, 60, 125, 250]:
            xvar = 'ret_p' + str(period)

            rs1 = rs.filter(lambda r: isnum(getattr(r, xvar)) and isnum(getattr(r, 'logsize')))

            y = [x * 100 for x in rs1.col(yvar)]
            size = rs1.col('logsize')
            ret = rs1.cols([xvar, 'vwret_p' + str(period)])

            x = []
            for s, r in zip(size, ret):
                x.append([1, s, (r[0] * 100 - r[1] * 100)])
            result = sm.OLS(y, x).fit()

            print("prev" , period, 'days', end=',')
            for x, pval in zip(result.params, result.pvalues):
                print(star(x, pval), end=',')

            print()
            print('tval', end=',')
            for tval in result.tvalues:
                print('[' + str(round(tval, 2)) + ']', end=',')
            print()
        print()

    for yvar in ['rpos', 'rneg', 'rnneg']:
        print('y var', yvar)
        print(',intercept,logsize,avg daily(mkt adj)')
        for period in [1, 2, 3, 5, 10, 20, 60, 125, 250]:
            xvar = 'ret_n' + str(period)

            rs1 = rs.filter(lambda r: isnum(getattr(r, xvar)) and isnum(getattr(r, 'logsize')))

            y = [x * 100 for x in rs1.col(yvar)]
            size = rs1.col('logsize')
            ret = rs1.cols([xvar, 'vwret_n' + str(period)])

            x = []
            for s, r in zip(size, ret):
                x.append([1, s, (r[0] * 100 - r[1] * 100)])
            result = sm.OLS(y, x).fit()

            print("next" , period, 'days', end=',')
            for x, pval in zip(result.params, result.pvalues):
                print(star(x, pval), end=',')

            print()
            print('tval', end=',')
            for tval in result.tvalues:
                print('[' + str(round(tval, 2)) + ']', end=',')
            print()
        print()


    print('y var: geometric daily avg ret(AFTER article)')
    for xvar in ['rpos', 'rneg', 'rnneg']:
        print(',intercept,logsize,' + xvar)
        for period in [1, 2, 3, 5, 10, 20, 60, 125, 250]:

            rs1 = rs.filter(lambda r: isnum(getattr(r, 'ret_n' + str(period))) and isnum(getattr(r, 'logsize')))

            y = [(x[0] - x[1]) * 100 for x in \
                 rs1.cols(['ret_n' + str(period), 'vwret_n' + str(period)])]
            x = sm.add_constant(rs1.cols(['logsize', xvar]))

            result = sm.OLS(y, x).fit()

            print("next" , period, 'days', end=',')
            for x, pval in zip(result.params, result.pvalues):
                print(star(x, pval), end=',')

            print()
            print('tval', end=',')
            for tval in result.tvalues:
                print('[' + str(round(tval, 2)) + ']', end=',')
            print()
        print()



    print('y var: avg daily turn over(trading vol / shrout) (AFTER article)')
    for xvar in ['diffop1', 'diffop2']:
        print(',intercept,logsize,' + xvar)
        for period in [1, 2, 3, 5, 10, 20, 60, 125, 250]:
            yvar = 'tnover_n' + str(period)

            rs1 = rs.filter(lambda r: isnum(getattr(r, yvar)) and \
                            isnum(getattr(r, 'logsize')) and isnum(getattr(r, xvar)))

            y = [x for x in rs1.col(yvar)]
            x = sm.add_constant(rs1.cols(['logsize', xvar]))

            result = sm.OLS(y, x).fit()

            print("next" , period, 'days', end=',')
            for x, pval in zip(result.params, result.pvalues):
                print(star(x, pval), end=',')

            print()
            print('tval', end=',')
            for tval in result.tvalues:
                print('[' + str(round(tval, 2)) + ']', end=',')
            print()
        print()


