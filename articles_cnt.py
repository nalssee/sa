"""count words in articles, from "articles" table
this one does more, but just articles_cnt table is important
all the others are just written when I did't have the "framework"

JUST LOOK AT THE "articles_cnt"
"""


from pydwork.sqlplus import *
import os
from itertools import islice
import re
import json
import time
import statistics
import requests


set_workspace('/Volumes/Seagate Backup Plus Drive/Seeking_Alpha/workspace')

NEGFILE = 'dictionary/LoughranMcDonald_Negative.csv'
POSFILE = 'dictionary/LoughranMcDonald_Positive.csv'


#First parameter is the replacement, second parameter is your input string

#Out: 'abdE'

def getwords(filename):
    with open(filename) as f:
        result = []
        for word in f.readlines():
            result.append(word[:-1])
        return result


with dbopen('space.db') as c:
    def year(r):
        return str(r.date)[:4]

    # for g in gby(c.reel('select * from articles order by date'), year):
    #     print(g.date[0], len(g.date))
    # for r in c.reel('select * from articles limit 10'):
    #     print(r.columns)
    # print(c.list_tables())

    # c.show('select * from articles where id_comment != 0 limit 10', filename='foo.csv')
    # c.show('select count(*) from articles0')
    # print(sample_article.upper())
    start = time.time()

    def articles_cnt():
        cnt = 0

        poswords = set(getwords(POSFILE))
        negwords = set(getwords(NEGFILE))
        regex = re.compile('[^A-Z]')
        def countwords(text):
            total, pos, neg = 0, 0, 0
            for tok in regex.sub(' ', text.upper()).split(' '):
                if len(tok) > 1:
                    if tok in poswords:
                        pos += 1
                    if tok in negwords:
                        neg += 1
                    total += 1
            return total, pos, neg

        for r in c.reel('select * from articles'):
            cnt += 1
            if cnt % 1000 == 0:
                print('count: ', cnt, time.time() - start)
            try:
                ntot, npos, nneg = countwords(r.maintext)
                r.ntot = ntot
                r.npos = npos
                r.nneg = nneg
                del[r.maintext]
                yield r
            except:
                continue

    # c.run('drop table if exists articles_cnt')
    # c.save(articles_cnt)
    # c.show('select count(*) from articles_cnt where type="main"')

    # <- USE THIS IF YOU GET TO REALLY WORK ON IT LATER
    # <- BUT I THINK ARTICLES_YMD1 IS OK AS WELL
    def articles_ymd():
        for r in c.reel('select * from articles_cnt'):
            try:
                date = str(r.date)
                r.yyyy = date[:4]
                r.mm = date[4:6]
                r.dd = date[6:]
            except:
                continue
            yield r
    # c.run('drop table if exists articles_ymd')
    c.save(articles_ymd)


    # del_header('ticker.csv')
    # add_header('ticker.csv', 'permno, date, shrcd, exchcd, ticker, comnam')
    # c.run('drop table if exists tickers')
    # c.save(read_csv('ticker.csv'), name='tickers')

    def tickers1():
        for r in c.reel("""
        select * from tickers
        where (shrcd=10 or shrcd=11 or shrcd=12)
        and (exchcd=1 or exchcd=2 or exchcd=3)
        group by ticker
        """):
            yield r

    # c.run('drop table if exists tickers1')
    # c.save(tickers1)
    def articles_ymd1():
        tickers = []
        for r in c.reel('select * from tickers1'):
            tickers.append(r.ticker)
        tickers_set = set(tickers)
        for r in c.reel('select * from articles_ymd'):
            if r.ticker in tickers_set:
                yield r
    # c.run('drop table if exists articles_ymd1')
    c.save(articles_ymd1)
    # c.show('select count(*) from articles_ymd1')
    # c.show("""
    # select count(*) from articles_ymd1 where yyyy >= 2005 and yyyy <= 2012
    # and type='main'
    # """)
    #
    # c.show("""
    # select count(*) from articles_ymd1 where yyyy >= 2013 and yyyy <= 2015
    # and type='main'
    # """)

    def articles_ymd2():
        for r in c.reel("""
        select * from articles_ymd1
        where yyyy >= 2005 and yyyy <= 2015
        and ntot > 0
        """):
            yield r

    # c.run("drop table if exists articles_ymd2")
    c.save(articles_ymd2)
    # c.show("select count(*) from articles_ymd2")

    # c.show("""
    # select count(*) from articles_ymd2 where type="comment"
    # """)

    def get_n_tickers():
        tickers = []
        for r in c.reel("""
        select * from articles_ymd2
        where type='comment'
        """):
            tickers.append(r.ticker)
        return len(set(tickers))

    # print(get_n_tickers())

    def get_avg():
        ntots = []
        nnegs = []
        for r in c.reel("""
        select * from articles_ymd2
        where type="comment"
        """):
            ntots.append(r.ntot)
            nnegs.append(r.nneg)

        avg = statistics.mean(ntots)
        stdev = statistics.stdev(ntots)
        nnegsr = [(neg/tot)*100.0 for tot, neg in zip(ntots, nnegs)]
        n_avg = statistics.mean(nnegsr)
        n_stdev = statistics.stdev(nnegsr)

        print('avg # of words', avg)
        print('stdev # of words', stdev)
        print('avg # of neg words percent', n_avg)
        print('stdev # of neg words percent', n_stdev)
    #
    # get_avg()

    def by_year():
        for g in gby(c.reel("""
        select * from articles_gender
        where type='main' and gender='female'
        order by yyyy, id_article
        """), 'yyyy'):
            print("%s & %s & %s & %s & %s & %s \\\\" %
                  (g.yyyy[0],
                   "{:,}".format(len(g.ntot)),
                   "{:,}".format(len(set(g.ticker))),
                   "{:,}".format(round(statistics.mean(g.ntot), 1)),
                   "{:,}".format(round(statistics.mean(g.nneg), 1)),
                   "{:,}".format(round(statistics.mean(g.npos), 1))))

    # by_year()

    def by_month():
        for g in gby(c.reel("""
        select * from articles_gender
        where type='main'
        order by yyyy, mm
        """), 'yyyy mm'):
            print("%s & %s & %s & %s & %s & %s \\\\" %
                  (str(g.yyyy[0]) + str(g.mm[0]).zfill(2),
                   "{:,}".format(len(g.ntot)),
                   "{:,}".format(len(set(g.ticker))),
                   "{:,}".format(round(statistics.mean(g.ntot), 1)),
                   "{:,}".format(round(statistics.mean(g.nneg), 1)),
                   "{:,}".format(round(statistics.mean(g.npos), 1))))

    # by_month()


    def first_names():
        names = []
        for r in c.reel("""
        select * from articles_ymd2
        """):
            try:
                name = r.authorname.split(' ')[0].upper()
                if re.match('^[A-Z]+$', name):
                    names.append(name)
            except:
                continue

        for name in set(names):
            r = Row()
            r.name = name.upper()
            yield r

    # c.show(first_names, filename='firstnames.csv')
    # c.show('select * from articles_ymd2 limit 2')

    # c.save(read_csv('first_names_final.csv'), name='first_names')

    # c.show("""select name, gender, count, probability from first_names where (gender == "male" or gender="female")
    # and count >= 3 and probability >= 0.6
    # """, filename="first_names_final1.csv")

    def articles_gender():
        gender = {}
        for r in c.reel("""
        select name, gender, count, probability from first_names
        where (gender == "male" or gender="female")
        and count >= 10 and probability >= 0.7
        order by count, probability
        limit 50
        """):
            gender[r.name] = r.gender
            print("%s & %s & %s & %s \\\\" % (r.name, r.gender, r.count, r.probability))


        # for r in c.reel("""
        # select * from articles_ymd2
        # """):
        #     try:
        #         name = r.authorname.split(' ')[0].upper()
        #         sex = gender[name]
        #         if sex:
        #             r.gender = sex
        #             yield r
        #     except:
        #         continue





    # articles_gender()


    # c.run('drop table if exists articles_gender')
    # c.save(articles_gender)
    # c.show('select count(*) from articles_gender where type="comment" and gender="female"')





    # os.system("say 'Your job is done'")
