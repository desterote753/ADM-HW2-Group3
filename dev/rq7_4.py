"""
This python scripts solve bulletpoint 4 of research question 7, i.e.:


"""

import pandas as pd
# import os
# import json
import zipfile
import warnings
from scipy.stats import fisher_exact


list_head = None

zip_path = "LargeBooksKaggle.zip"
zip_path = r"F:/adm/LargeBooksKaggle.zip" # TODO: Delete this line!

json_path = r"list.json/list.json"
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    with zip_ref.open(json_path) as f:
        list_head = pd.read_json(f, lines=True, nrows = 1) # assuming there is just one list called "The Worst Books of All Time"

# assessing the books inside the list "The Worst Books of All Time".
worst_books_of_all_time = list_head[list_head['title']=="The Worst Books of All Time"]["books"]
if len(worst_books_of_all_time) == 1:
    worst_books_of_all_time = worst_books_of_all_time[0]
else:
    if len(worst_books_of_all_time) < 1:
        warnings.warn("Less than one entry")
        # raise Exception("invalid data")
    if len(worst_books_of_all_time) > 1:
        warnings.warn("More  than 1 entry")

# assessing the book_ids and deleting duplicates
worst_book_ids_of_all_time = [ entry['book_id'] for entry in worst_books_of_all_time ] # assessing the book_ids 
if len(worst_book_ids_of_all_time) > len(set(worst_book_ids_of_all_time)):
    worst_book_ids_of_all_time = list(set(worst_book_ids_of_all_time)) # to eliminate duplicates. We checked there are no duplicates.


processed_rows = 0
contingency_table = None
flag = True

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
            chunk = chunk[["id","num_pages"]] # to shrink needed memory
            chunk = chunk[chunk["num_pages"]!=""] # to exclude books without pages mentioned.
            chunk.astype({'num_pages':'int'})
            chunk["gt700"] = chunk.apply( lambda row: row['num_pages'] > 700 , axis=1)
            chunk["oneOftheWorst"] = chunk.apply(lambda row : str(row['id']) in worst_book_ids_of_all_time, axis=1) # TODO: str() is necessary. It would be better if both were ints. But this raises errors.        
            if flag:
                contingency_table = pd.crosstab(chunk['oneOftheWorst'], chunk['gt700'])
                flag = False
            else:
                contingency_table += pd.crosstab(chunk['oneOftheWorst'], chunk['gt700'])
            print(processed_rows) # in total
            print('current contingency table looks like:\n', contingency_table)




_, pval = fisher_exact(contingency_table)
alpha = 0.05
print('assuming a test-level of alpha=', alpha)
strres = ""
if pval < alpha:
    strres = "there's significance for dependence"
else:
    strres = "there's no significance for dependence"

print("Since the pvalue is", pval, strres)