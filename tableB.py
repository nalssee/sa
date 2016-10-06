from pydwork.sqlplus import *
from pydwork.util import isnum

import statsmodels.api as sm

import random
set_workspace('workspace')

PERIODS = [1, 2, 3, 5, 10, 20, 60, 125, 250]

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

    rs = Rows(c.reel("""
        select * from datasetB
        where
        date <= 20151231
        """))

    for yvar in ['rpos', 'rneg', 'rnneg']:
        print('y var', yvar)
        print(',intercept,logsize,avg daily(mkt adj)')
        for period in PERIODS:
            rs1 = []
            ret_pstr = 'ret_p' + str(period)
            vwret_pstr = 'vwret_p' + str(period)
            for r in rs:
                ret = getattr(r, ret_pstr)
                mret = getattr(r, vwret_pstr)
                if isnum(ret) and isnum(mret) \
                   and isnum(r.logsize) and isnum(getattr(r, yvar)):
                    r.adjret_pn = ret - mret
                    rs1.append(r)

            # truncate version
            # rs1 = Rows(rs1).truncate('adjret_pn', 0.01)
            rs1 = Rows(rs1)

            y = rs1.col(yvar)
            x = sm.add_constant(rs1.cols(['logsize', 'adjret_pn']))
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


    print("reg with (market and momemtum) less previous")
    for yvar in ['rpos', 'rneg', 'rnneg']:
        print('y var', yvar)
        print(',intercept,ret prev')
        for period in PERIODS:

            rs1 = Rows(rs).num([yvar, 'ret_less2_p' + str(period)])
            Y = rs1.col(yvar)
            X = sm.add_constant(rs1.cols(['ret_less2_p' + str(period)]))
            result = sm.OLS(Y, X).fit()

            print("prev" , period, 'days', end=',')
            for x, pval in zip(result.params, result.pvalues):
                print(star(x, pval), end=',')

            print()
            print('tval', end=',')
            for tval in result.tvalues:
                print('[' + str(round(tval, 2)) + ']', end=',')
            print()
        print()


    print("reg with 4 factor less previous")
    for yvar in ['rpos', 'rneg', 'rnneg']:
        print('y var', yvar)
        print(',intercept,ret prev')
        for period in PERIODS:

            rs1 = Rows(rs).num([yvar, 'ret_less4_p' + str(period)])
            Y = rs1.col(yvar)
            X = sm.add_constant(rs1.cols(['ret_less4_p' + str(period)]))
            result = sm.OLS(Y, X).fit()

            print("prev" , period, 'days', end=',')
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
        for period in PERIODS:

            rs1 = []
            ret_pstr = 'ret_n' + str(period)
            vwret_pstr = 'vwret_n' + str(period)
            for r in rs:
                ret = getattr(r, ret_pstr)
                mret = getattr(r, vwret_pstr)
                if isnum(ret) and isnum(mret) \
                   and isnum(r.logsize) and isnum(getattr(r, xvar)):
                    r.adjret_nn = ret - mret
                    rs1.append(r)
            # truncate version
            # rs1 = Rows(rs1).truncate('adjret_nn', 0.01)
            rs1 = Rows(rs1)

            y = rs1.col('adjret_nn')
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


    print("reg with (market and momemtum) less next")
    for xvar in ['rpos', 'rneg', 'rnneg']:
        print('y var', 'ret next')
        print(',intercept,%s' % (xvar,))
        for period in PERIODS:
            yvar = 'ret_less2_n' + str(period)

            rs1 = Rows(rs).num([xvar, yvar])
            Y = rs1.col(yvar)
            X = sm.add_constant(rs1.cols([xvar]))
            result = sm.OLS(Y, X).fit()

            print("next" , period, 'days', end=',')
            for x, pval in zip(result.params, result.pvalues):
                print(star(x, pval), end=',')

            print()
            print('tval', end=',')
            for tval in result.tvalues:
                print('[' + str(round(tval, 2)) + ']', end=',')
            print()
        print()

    print("reg with 4 factor less next")
    for xvar in ['rpos', 'rneg', 'rnneg']:
        print('y var', 'ret next')
        print(',intercept,%s' % (xvar,))
        for period in PERIODS:
            yvar = 'ret_less4_n' + str(period)

            rs1 = Rows(rs).num([xvar, yvar])
            Y = rs1.col(yvar)
            X = sm.add_constant(rs1.cols([xvar]))
            result = sm.OLS(Y, X).fit()

            print("next" , period, 'days', end=',')
            for x, pval in zip(result.params, result.pvalues):
                print(star(x, pval), end=',')

            print()
            print('tval', end=',')
            for tval in result.tvalues:
                print('[' + str(round(tval, 2)) + ']', end=',')
            print()
        print()





    #
    print('y var: avg daily turn over(trading vol / shrout) (AFTER article)')
    for xvar in ['diffop1', 'diffop2']:
        print(',intercept,logsize,' + xvar)
        for period in PERIODS:
            rs1 = Rows(rs).num([xvar, 'logsize', 'tnover_n' + str(period)])

            Y = rs1.col('tnover_n' + str(period))
            X = rs1.cols(['logsize', xvar])

            result = sm.OLS(Y, sm.add_constant(X)).fit()

            print("next" , period, 'days', end=',')
            for x, pval in zip(result.params, result.pvalues):
                print(star(x, pval), end=',')

            print()
            print('tval', end=',')
            for tval in result.tvalues:
                print('[' + str(round(tval, 2)) + ']', end=',')
            print()
        print()


