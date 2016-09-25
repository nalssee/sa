from pydwork.sqlplus import *


set_workspace('workspace')


with dbopen('space.db') as c:
    # c.drop('articles_prets')
    c.save(reel('articles_prets'), name='articles_prets')
    # c.show('select count(*) from articles_prets')
    c.show('articles_prets')

    def datasetB():
        for rs in c.reel(
            """
            select * from articles_prets
            order by id_article
            """, group='id_article'):


