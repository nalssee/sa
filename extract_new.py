""" 긁은 파일에서 필요한 것들만 뽑아내기
"""


from pydwork.sqlplus import *

import pandas as pd
import os
from bs4 import BeautifulSoup
from datetime import datetime

# where results files are
RESULT_DIR = '/Volumes/Seagate Backup Plus Drive/Seeking_Alpha/rawdata2014_2015'
# I think even workspace must be in external storage, it's too huge
WORK_DIR = '/Volumes/Seagate Backup Plus Drive/Seeking_Alpha'




# 2014년 4월정도까지만 받았었기 때문에 그 이후 것들을 더해줘야해
def get_htmls(file):
    " result 파일로부터 html 스트링들을 뽑아낸다"
    df = pd.read_csv(file)
    for r in df.iterrows():
        yield r[1].address, r[1].page

def get_result_files(folder):
    for file in os.listdir(folder):
        if file.startswith('result'):
            yield os.path.join(folder, file)

def save_html(address, html, ext='.html'):
    with open(os.path.join(RESULT_DIR, address[9:].strip() + ext), 'w') as f:
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
    for p in elt.find_all(['p', 'li']):
        ps.append(p.text)
    return '\n'.join(ps)

def read_date_main(date):
    "read date formate in main articles, 2014-05-04T20:10:19Z"
    return date[:10].replace('-', '')

def read_date_comment(date):
    """이 빌어먹을게 코멘트는 날짜 형식을 다르게 쓰는데 이게 또 일정치가 않을거야 아마
    05 May 2014, 06:34 PM
    """
    date = date[:11]
    dateobj = datetime.strptime(date[:11], '%d %b %Y')
    return dateobj.strftime('%Y%m%d')


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
            return get_element('div#a-hd h1', lambda elt: elt.text)

        def get_date():
            # 바로 time 으로 골라 들어가면 안되네. 이유를 잘 모르겠삼
            date = get_element('div.a-info',
                               lambda elt: elt.select('time')[0]['content'])
            return read_date_main(date)

        def get_ticker():
            return get_element('span#about_primary_stocks a',
                                lambda elt: elt['href'][8:])

        def get_authorname():
            return get_element('a.name-link',
                                lambda elt: elt.select('span.name')[0].text)

        def get_maintext():
            return get_element('div#a-cont', onlytext)


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
            date = get_element('span.c-pub span', lambda elt: elt.text)
            return read_date_comment(date)

        def get_maintext():
            return get_element('div.b-c-content',
                               lambda elt: elt.text)

        def get_authorname():
            return get_element('a.c-nick',
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


    soup = BeautifulSoup(html, 'html.parser')
    return [main_article(soup)] + comments(soup)



if __name__ == '__main__':
    # with dbopen("workspace.db") as conn:
    #     conn.show("select * from articles where commentno != 'none'", n=10)

    # make_samples("sample/result2016-05-25 10:25:00.827396.csv")
    with dbopen(os.path.join(WORK_DIR, 'workspace_new.db')) as conn:
        # 2014년 전체는 아니고 2014 년 4월정도 부터야
        @adjoin(['type', 'address', 'title', 'date', 'maintext', 'authorname', 'ticker'])
        def articles():
            succeeded = 0
            failed = 0
            for result_file in get_result_files(RESULT_DIR):
                for address, html in get_htmls(result_file):
                    try:
                        for r in scrap_html(address, html):
                            yield r
                        succeeded += 1
                    except:
                        failed += 1
                        continue
            print('succeeded: ', succeeded)
            print('failed: ', failed)

        # 아래줄('drop table') 은 나중에 지워주고
        # conn.run('drop table if exists articles')
        conn.save(articles)
        # for r in conn.reel('select * from articles'):
        #     save_html(r.address, r.main_text, ext='.txt')
        # conn.show('select * from articles', filename='sample.csv')
        conn.show('select * from articles where type="main"')

        # conn.show('pragma table_info(article2014)')
