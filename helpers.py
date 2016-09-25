import numpy as np
import matplotlib.pyplot as plt
import sys

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pydwork.sqlplus import *

# 간단한 히스토그램
def hist_simp(data):
    maxval = max(data)
    minval = min(data)
    binwidth = (maxval - minval) / 100

    data1 = np.array(data)
    for i in range(0, 101, 5):
        print("%3d %%: %.4f" % (i, np.percentile(data1, i)))

    plt.hist(data1, bins=np.arange(minval, maxval + binwidth, binwidth))
    plt.show()

# 간단한 scatter plot



def isnum(x):
    return isinstance(x, float) or isinstance(x, int)


# 두 수의 percentage difference
def pdiff(x, y):
    if x == y:
        return 0
    # 절대값은 같고 부호만 다르면
    elif x + y == 0:
        return sys.maxsize
    else:
        return abs((x - y) / ((x + y) / 2)) * 100


def yyyymm(date, n):
    d1 = datetime.strptime(str(date), '%Y%m') + relativedelta(months=n)
    return d1.strftime('%Y%m')


# yields duplicates from a sequence
def dups(seq, cols):
    for rs in Rows(seq).order(cols).group(cols):
        if len(rs) > 1:
            yield from rs
