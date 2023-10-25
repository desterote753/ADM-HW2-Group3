"""
This python scripts solve bulletpoint 3 of research question 7, i.e.:
In the file list.json, you will find a peculiar list named "The Worst Books of All Time." Estimate the probability of a book being included in this list, knowing it has more than 700 pages.
"""

import pandas as pd
# import os
# import json
import zipfile
import warnings

list_head = None

zip_path = "LargeBooksKaggle.zip"
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
books_with_gt_700_pages_count = 0 # interpreting "more than 700 pages" as "strictly greater than ..."
worst_books_with_gt_700_pages_count = 0

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
            chunk.astype({'num_pages':'int'}) # was not possible when reading the .json-file, because of ""-entries
            chunk = chunk[chunk["num_pages"]>700] # to exclude books with <= 700 pages 
            books_with_gt_700_pages_count += len(chunk)
            chunk["oneOftheWorst"] = chunk.apply(lambda row : str(row['id']) in worst_book_ids_of_all_time, axis=1) # TODO: str() is necessary. It would be better if both were ints. But this raises errors. Are there ""-ids? i.e. empty ids?
            worst_books_with_gt_700_pages_count += len(chunk[chunk["oneOftheWorst"]])
            print("processed rows:", processed_rows) # in total
            print("current estimated probability:", worst_books_with_gt_700_pages_count /  books_with_gt_700_pages_count)

print(worst_books_with_gt_700_pages_count)
print(books_with_gt_700_pages_count)
print(worst_books_with_gt_700_pages_count / books_with_gt_700_pages_count)