import pandas as pd


def parseToDict(mystr):
    myli = mystr.split("|")
    mydict = {}
    for entry in myli:
        k,v = entry.split(":")
        try:
            # k = int(k)
            v = int(v)
        except ValueError as e:
            print(e)
            continue
        mydict[k] = v
        # from collections import Counter
    
    return mydict

def getPercentage(my_dict, key_numerator, key_denominator):
    try:
        return my_dict[key_numerator]/my_dict[key_denominator] # we interpret above 4 as strictly above 4.
    except ZeroDivisionError as e:
        print(e)
        return None


path = "lighter_books_updated" +".json"

books_with_30Percent_of_ratings_above_4 = 0
books_with_at_least_one_rating = 0
# load config
processed_rows = 0

chunks = pd.read_json(path, lines=True, chunksize = 10**4, nrows = None)
for chunk in chunks:
    chunk = chunk[["id","ratings_count", "rating_dist"]] # to shrink needed memory
    chunk = chunk[chunk["ratings_count"]>0] # to exclude books without ratings
    books_with_at_least_one_rating += len(chunk)
    chunk["percentage"] = chunk.apply(lambda row : getPercentage(parseToDict(row['rating_dist']),'5','total'), axis=1)        
    books_with_30Percent_of_ratings_above_4 += len(chunk[chunk["percentage"] > 0.3]) # we interpret over as strictly over
    # print(chunk)
    processed_rows += len(chunk)
    print(processed_rows) 

print(books_with_30Percent_of_ratings_above_4)
print(books_with_at_least_one_rating)
print(books_with_30Percent_of_ratings_above_4 / books_with_at_least_one_rating)