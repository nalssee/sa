# merge old and new articles together

from pydwork.sqlplus import *

import pandas as pd
import os
from datetime import datetime

WORK_DIR = '/Volumes/Seagate Backup Plus Drive/Seeking_Alpha'
RESULT_DIR1 = '/Volumes/Seagate Backup Plus Drive/Seeking_Alpha/rawdata'


def inwork(path):
    return os.path.join(WORK_DIR, path)



def save_html(address, html, ext='.html'):
    with open(os.path.join('sample', address[9:].strip() + ext), 'w') as f:
        f.write(html)



# with dbopen(inwork('workspace.db')) as conn:
#     with dbopen(inwork('workspace_old.db')) as conn0:
#         conn.save(conn0.reel('select * from articles'), name='articles_old')
#     with dbopen(inwork('workspace_new.db')) as conn0:
#         conn.save(conn0.reel('select * from articles'), name='articles_new')

def tag_id(conn, tablename):
    """
    conn(sqlplus.SQLPlus): connection
    tablename(str)
    => rows(iter): article_id columns is added
    """
    def get_numeral(address):
        """
        In:
            address(str) : /article/62-nbc-and-cinemanow-sign-distr...
        Out:
            numberal part of address : '62'
        """
        return address[9:].split('-')[0]

    for g in gby(conn.reel('select * from %s' % tablename), 'address', bind=False):
        for i, r in enumerate(g):
            r.article_id = get_numeral(r.address) + '_' + str(i)
            yield r


with dbopen(inwork('workspace.db')) as conn:
    def articles0():
        rows = conn.reel("""
        select * from articles order by address
        """)
        for g in gby(rows, 'address', bind=False):
            new_rs = []
            for r in g:
                r.id_article, r.id_comment = r.article_id.split('_')
                new_rs.append(r)
            for r in sorted(new_rs, key=lambda x: int(x.id_comment)):
                yield r
    # conn.run('drop table if exists articles0')
    # conn.save(articles0)

    # conn.run("""
    # create table articles1 as
    # select type, id_article, id_comment, address, date, ticker, authorname, title, maintext
    # from articles0
    # order by id_article, id_comment
    # """)

    # conn.show("select * from articles1", n=100)
    # conn.show("select * from sqlite_master")
    # conn.run("drop table if exists articles")
    # conn.run("drop table if exists articles0")
    # conn.run('alter table articles1 rename to articles')

    #
    # conn.run('vacuum')






    os.system("say 'Your job is done'")
