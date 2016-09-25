# This one just does the same as what wrangle.py does
# But I think after the renewal of the site,
# article formats have been changed a bit
# so you can't simply apply the same script to older raw files
# and raw data is in sqlite not csv


from pydwork.sqlplus import *

import pandas as pd
import os
from bs4 import BeautifulSoup
from datetime import datetime


# where results files are
RESULT_DIR = '/Volumes/Seagate Backup Plus Drive/Seeking_Alpha/rawdata'
# I think even workspace must be in external storage, it's too huge
WORK_DIR = '/Volumes/Seagate Backup Plus Drive/Seeking_Alpha'

LINKS = ['links000k.db', 'links100k.db', 'links200k.db', 'links300k.db']

# This is for testing purposes
def make_some_samples(db_file, n=100):
    """
    db_file(str): database file name
    n(int): number of samples to generate
    =>
    None: as a side effects generate some html files with a name of
    'article address' in 'samples' folder (current directory)
    """
    with dbopen(os.path.join(RESULT_DIR, db_file)) as conn:
        for r in conn.reel('select * from sahtml limit ?', args=(n,)):
            save_html(r.address, r.html)



def save_html(address, html, ext='.html'):
    with open(os.path.join('sample', address[9:].strip() + ext), 'w') as f:
        f.write(html)




def onlytext(elt):
    """
    elt: BeautifulSoup object
    return: string
    ----------------------------------------

    extracts only text(so you can read) from html tag
    This works only for the main text
    """
    ps = []
    # A few wierd articles contain text directly in 'div' tag.
    # However I am ignoring these to keep accordance with the new(2014-2015) articles
    for p in elt.find_all(['p', 'li']):
        ps.append(p.text)
    return '\n'.join(ps)

def read_date_main(date):
    """ read date format in main articles,
    Jun. 18, 2012  4:55 AM ET
    """
    dateobj = datetime.strptime(date[:13], '%b. %d, %Y')
    return dateobj.strftime('%Y%m%d')

def read_date_comment(date):
    """
    05 May 2014, 06:34 PM
    이 미친 썅놈의게 지금 다운로드하면 고쳐졌는데, 옛날에 받을 때는 year 가 빠져있어
    그러니까
    05 May, 06:34 PM
    year 빠지고 이따위로 나오는 건 다 2014년 거야
    어떻게 알았냐고? attribute 중에 unix epoch seconds 정보가 나와
    timezone 조정하면 그렇다는 걸 알수있어
    """
    try:
        dateobj = datetime.strptime(date[:11], '%d %b %Y')
        return dateobj.strftime('%Y%m%d')
    except:
        dateobj = datetime.strptime(date[:6], '%d %b')
        return '2014' + dateobj.strftime('%m%d')

# print(read_date_comment('05 May 2014, 06:34 PM'))
# print(read_date_comment('05 May, 06:34 PM'))
# print(read_date_comment('21 Feb, 08:44 PM'))


def scrap_html(address, html):
    """HTML 에서 필요로 하는 걸 뽑아낸 후, Row 로 전환
    """
    # 이거 지워 나중에

    ticker = None
    def main_article(soup):

        nonlocal ticker

        def get_element(css, nextfn):
            elts = soup.select(css)
            if elts == []:
                raise
            else:
                return nextfn(elts[0])

        def get_title():
            return get_element('div#page_header h1 span', lambda elt: elt.text)

        def get_date():
            # 바로 time 으로 골라 들어가면 안되네. 이유를 잘 모르겠삼
            date = get_element('div#article_info div.article_info_pos span',
                                lambda elt: elt.text)
            return read_date_main(date)

        def get_ticker():
            return get_element('span#about_primary_stocks a',
                                lambda elt: elt['href'][8:])

        def get_authorname():
            return get_element('a#author_info_name',
                                lambda elt: elt.text)

        def get_maintext():
            return get_element('div#article_body', onlytext)


        r = Row()
        # 원래 integer 로 해야 하는데, 그냥 쉽게 가자
        r.type = 'main'
        r.address = address
        r.title = get_title()
        # date of publication
        r.date = get_date()
        r.maintext = get_maintext()
        r.authorname = get_authorname()
        r.ticker = get_ticker()

        ticker = r.ticker
        return r


    def comments(soup):
        elts = soup.select("section#comments div.c-list div.c-body")

        comments = []
        for elt in elts:
            comments.append(comment1(elt))
        return comments

    def comment1(soup):
        def get_element(css, nextfn):
            elts = soup.select(css)
            if elts == []:
                pass
            else:
                return nextfn(elts[0])

        def get_date():
            date = get_element('span.comment_time', lambda elt: elt.text)
            return read_date_comment(date)

        def get_maintext():
            return get_element('span.cont_com',
                               lambda elt: elt.text)

        def get_authorname():
            return get_element('a.commenter_name',
                               lambda elt: elt.text)


        r = Row()
        r.type = 'comment'
        r.address = address
        r.title = None
        r.date = get_date()
        r.maintext = get_maintext()
        r.authorname = get_authorname()
        r.ticker = ticker
        return r

    def comments(soup):
        elts = soup.select("div#comment_container li")

        comments = []
        for elt in elts:
            comments.append(comment1(elt))
        return comments


    soup = BeautifulSoup(html, 'html.parser')
    return [main_article(soup)] + comments(soup)


if __name__ == '__main__':
    # make_some_samples('links300k.db')

    with dbopen(os.path.join(WORK_DIR, 'workspace_old.db')) as conn:
        @adjoin(['type', 'address', 'title', 'date', 'maintext', 'authorname', 'ticker'])
        def articles():
            succeeded = 0
            failed = 0
            for linkdb in LINKS:
                with dbopen(os.path.join(RESULT_DIR, linkdb)) as conn:
                    for r0 in conn.reel('select * from sahtml'):
                        try:
                            for r in scrap_html(r0.address, r0.html):
                                yield r
                            succeeded += 1
                            if succeeded % 100 == 0:
                                print(succeeded, failed)

                            # save_html(r0.address, r0.html)
                        except:
                            failed +=1
                            # save_html(r0.address, r0.html)
                            continue

            print('succeeded: ', succeeded)
            print('failed: ', failed)

        conn.run('drop table if exists articles')
        conn.save(articles)
