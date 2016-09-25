# two way sort

# dependent sort first
def twoway(seq, col1, nport1,
           col2, nport2,
           weightfn,
           datecol='yyyymm'):

    for rs in seq.order([datecol, col]).group(datecol):
        # todo!!


    pass






