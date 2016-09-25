from pydwork.pullin import *
from pydwork.sqlplus import *
import requests
import os
import json
import pandas as pd
from itertools import *

# WS = '/Volumes/Seagate Backup Plus Drive/Seeking_Alpha/workspace'

drivers = []
for _ in range(10):
    drivers.append(requests.get)

def grouper(n, iterable, fillvalue=''):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)

def build_items():
    with open('firstnames.csv') as f:
        f.readline()
        names = []
        for name in f:
            names.append('name[]=' + name[:-1])
        names10 = []
        for g in grouper(10, names):
            names10.append("https://api.genderize.io/?apikey=c7ddf6b7772306503af8f8b086a3e2ce&" + '&'.join(g))
        return names10

def fetchfn(driver, item):
    resp = driver(item)

    names = []
    genders = []
    probabilities = []
    counts = []

    for info in json.loads(resp.text):
        names.append(info.get('name', None))
        genders.append(info.get('gender', None))
        probabilities.append(info.get('probability', None))
        counts.append(info.get('count', None))
    return pd.DataFrame({
        'name': names,
        'gender': genders,
        'probability': probabilities,
        'count': counts
    })

# fetch_items(drivers, build_items(), fetchfn)
# result_files_to_df().to_csv('first_names_final.csv')
