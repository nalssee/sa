# 가져오기가 실패한 구간이 있는데, 그 부분에 대해서 다시 한번 시도해보자.
# page columns 내용이 str 이 아닌 것들을 뽑아서 다시 links2014_failed.csv 라는
# 이름의 파일을 만들자
# 그리고 그 링크들을 다시 가져오기하면 된다

import os
import pandas as pd
DEST_DIR = '/Volumes/Seagate Backup Plus Drive/Seeking_Alpha/rawdata2/backup/'

with open('links2014_failed.csv', 'w') as f :
    c = 0
    for file in os.listdir(DEST_DIR):
        if file.startswith('result'):
            df = pd.read_csv(os.path.join(DEST_DIR, file))
            for r in df.iterrows():
                if not isinstance(r[1].page, str):
                    f.write(r[1].address)
                    print(r[1].address)
                    c += 1
    print('total failure', c)
