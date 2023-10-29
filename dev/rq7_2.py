"""
This python scripts solve bulletpoint 2 of research question 7, i.e.:



"""

import pandas as pd
# import os
# import json
import zipfile
import warnings
from datetime import datetime

def myfun2(*args, **kwargs):
    return_value = None
    res = list(*args, **kwargs)
    print(res)
    res = [ myStringToDate(entry, '%Y-%m-%d') for entry in res ]
    res = [ entry for entry in res if entry is not None ]
    res = [ myDateToSeconds(res[0], entry) for entry in res]
    res = sorted(set(res)) # first eliminate duplicates, then sort
    return_value = []
    for i in range(len(res)-1):
        return_value.append(res[i+1]-res[i])
    if len(return_value)>0:
        return_value = sum(return_value)/len(return_value) <= 2*60*60*24*365 # returns true if the author is expected to publish a new book within (<=) 2 years
    return return_value

def myStringToDate(date_string, date_format):
    res = None
    try:
        res = datetime.strptime(date_string, date_format)
    except ValueError as e:
        print(e)
    return res

def myDateToSeconds(initial_date, current_date):
    res = None
    try:
        res = current_date - initial_date
        res = res.total_seconds() 
    except Exception as e:
        print(e)
    return res




processed_rows = 0
df = None
flag = True

zip_path = "LargeBooksKaggle.zip"
# zip_path = "F:/adm/LargeBooksKaggle.zip"
# json_path = r"authors.json/authors.json"
json_path = r"books.json/books.json" # setting a new json_path
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    with zip_ref.open(json_path) as f:
        # print("hello")
        # print(f.readline())
        chunks = pd.read_json(f, lines=True, chunksize = 10**4, nrows = None, dtype=None )
        for chunk in chunks:
            processed_rows += len(chunk)
            if flag:
                df = chunk[['author_id','original_publication_date']] # ['author_id','original_publication_date']
                flag = False
            else:
                df = pd.concat([df, chunk[['author_id','original_publication_date']]  ], axis=0)
            print('processed rows:', processed_rows)

df_new = pd.DataFrame(df.groupby('author_id')['original_publication_date'].apply(myfun2))


def listlen(*args,**kwargs):
    return len(list(*args))

results = df_new[ (df_new['original_publication_date'] == True) | (df_new['original_publication_date'] == False)].groupby('original_publication_date')['original_publication_date'].apply(listlen)
# df_new[ df_new['original_publication_date'] == True]
# pd.DataFrame(results)
print(results.loc[True]/(results.loc[True]+results.loc[False]), 'is the probability that an author publishes a book within 2 years') # probability
