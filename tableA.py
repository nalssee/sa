from pydwork.sqlplus import *
from oneway import build_portfolios, print_avg, print_reg_tseries
import matplotlib.pyplot as plt

set_workspace('/Users/kenjin/pw/sa/workspace')


def main():
    with dbopen('space.db') as c:
        def dataset(start, end):
            result = []
            for r in c.reel(
                """
                select * from dataset
                where
                (shrcd = 10 or shrcd = 11) and
                (exchcd = 1 or exchcd = 2 or exchcd = 3) and
                isnum(avgrneg) = 1 and
                yyyy >= ? and yyyy < ?
                """, (start, end)):
                result.append(r)
            return result

        dsets = []
        for year in range(2006, 2016):
            dsets.append((dataset(year, year + 1), str(year)))
        dsets.append((dataset(2006, 2009), '2006-2008'))
        dsets.append((dataset(2009, 2016), '2009-2015'))
        dsets.append((dataset(2006, 2016), '2006-2015'))

        def plot(nport, col, weight):
            for seq, label in dsets[-1:]:
                ptable = build_portfolios(seq, [col],
                                          nport, weight=weight)
                rhigh = ptable.col('ret' + str(nport))
                rlow = ptable.col('ret1')
                mkt_rf = ptable.col('mkt_rf')
                rf = ptable.col('rf')
                yyyymm = ptable.col('yyyymm')
                mom = ptable.col('mom')
                mkt = [a + b for a, b in zip(mkt_rf, rf)]
                rhl = [a - b for a, b in zip(rhigh, rlow)]
                date = list(range(len(yyyymm)))
                plt.xticks(date, ['' for a in yyyymm])
                plt.plot(date, rhl, 'r-', date, mom, 'bs-')

#        plot(10, 'avgrneg', 'eq')
#        plt.show()
#
        # todo: check the validity of dataset, especially
        # newly added factors!!
        # I believe that maybe just checking if I can reproduce the previous
        # results!!

        #c.show('dataset')

        def oneway(nport, col, weight):
            for seq, label in dsets[-1:]:
                ptable = build_portfolios(seq, [col],
                                          nport, weight=weight,
                                          factors=['mkt_rf', 'smb',
                                                   'smbf', 'hml', 'rmw', 'cma',
                                                   'mom'])
                print(','.join([''] + [str(x) for x in range(1, nport + 1)] + ['H-L']))

                print_avg(ptable, 'ret', nport,
                          label=label,
                          print_header=False,
                          print_high_low=True)
                print()
                print_reg_tseries(ptable, ['mkt_rf'],
                                  nport, print_header=False,
                                  print_tvalue=True,
                                  print_intercept_only=False,
                                  intercept_label='Intercept')
                print()
                print_reg_tseries(ptable, ['mkt_rf', 'smb', 'hml'],
                                  nport, print_header=False,
                                  print_tvalue=True,
                                  print_intercept_only=False,
                                  intercept_label='Intercept')
                print()
                print_reg_tseries(ptable, ['mkt_rf', 'smb', 'hml', 'mom'],
                                  nport, print_header=False,
                                  print_tvalue=True,
                                  print_intercept_only=False,
                                  intercept_label='Intercept'
                                 )

                print()
                print_reg_tseries(ptable, ['mkt_rf', 'smbf', 'hml', 'rmw',
                                           'cma'],
                                  nport, print_header=False,
                                  print_tvalue=True,
                                  print_intercept_only=False,
                                  intercept_label='Intercept'
                                 )

                print()
                print_reg_tseries(ptable, ['mkt_rf', 'smbf', 'hml', 'rmw',
                                           'cma', 'mom'],
                                  nport, print_header=False,
                                  print_tvalue=True,
                                  print_intercept_only=False,
                                  intercept_label='Intercept'
                                 )



        print()
        print('Negative words, equally weighted')
        oneway(10, 'avgrneg', 'eq')
        print()
        print('Positive words, equally weighted')
        oneway(10, 'avgrpos', 'eq')
        print()
        print('Net Negative words, equally weighted')
        oneway(10, 'avgrnneg', 'eq')


        print()
        print('Negative words, value weighted')
        oneway(10, 'avgrneg', 'val')

        print()
        print('Positive words, value weighted')
        oneway(10, 'avgrpos', 'val')

        print()
        print('Net Negative words, value weighted')
        oneway(10, 'avgrnneg', 'val')


#        print()
#        print('Negative words, equally weighted')
#        oneway(5, 'avgrneg', 'eq')
#        print()
#        print('Negative words, value weighted')
#        oneway(5, 'avgrneg', 'val')
#
#        print()
#        print('Positive words, equally weighted')
#        oneway(5, 'avgrpos', 'eq')
#        print()
#        print('Positive words, value weighted')
#        oneway(5, 'avgrpos', 'val')
#
#        print()
#        print('Net Negative words, equally weighted')
#        oneway(5, 'avgrnneg', 'eq')
#        print()
#        print('Net Negative words, value weighted')
#        oneway(5, 'avgrnneg', 'val')
#


main()
