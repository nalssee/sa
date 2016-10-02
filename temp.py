from pydwork.sqlplus import *
from pydwork.util import isnum, pmap

import statistics as st
from itertools import islice
prod = 1
n = 0

xs = [
0.009484,
0.008491,
-0.002687,
0.00503,
0.010724
]


for x in islice(xs, 2):
    prod *= x + 1
    n += 1
print(prod, n)
print(prod ** (1 / n) - 1)

vol = [
5333767,
5143787,
4238546,
4345474,
4089539,
10026106,
5617475,
4263828,
5164916,
1495226
]
shrout = [
970110,
970110,
970110,
970110,
970110,
970110,
970110,
970110,
970110,
970110
]
sum = 0
n = 0
for v, s in islice(zip(reversed(vol), reversed(shrout)), 3):
    sum += v / s
    n += 1
print(sum / n)
