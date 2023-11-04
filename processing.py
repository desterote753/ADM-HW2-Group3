import functions
import numpy as np
import pandas as pd
from time import perf_counter
from datetime import datetime
import os
import warnings
from scipy.stats import fisher_exact # with this we can perform the fisher test
from scipy.stats import ks_2samp # kolmogorov-smirnov-two-sample-test
from sklearn.linear_model import LinearRegression

chunksize = 10**3
nrows = None

def initialize_processing_parameters_from_config():
    global chunksize
    chunksize = functions.get_from_config('chunksize')
    print('chunksize set to', chunksize)
    global nrows
    nrows = functions.get_from_config('nrows')
    print('nrows set to', nrows)

### RQ_7_1

def answer_rq_7_1(data_path):
    # chunksize = 2*10**5
    # nrows = None
    processed_rows = 0
    books_with_at_least_one_rating = 0
    books_with_30Percent_of_ratings_above_4 = 0

    start = perf_counter()
    for chunk in pd.read_json( os.path.join( *data_path, "lighter_books" + ".json"), lines=True, chunksize=chunksize, nrows=nrows):
        processed_rows += len(chunk)
        chunk = chunk[["id","ratings_count", "rating_dist"]] # to shrink needed memory
        chunk = chunk[chunk["ratings_count"]>0] # to exclude books without ratings
        chunk["percentage"] = chunk.apply(lambda row : functions.getRatio(functions.checkValidityRQ7_1(functions.parseToDict(row['rating_dist'])),['4','5'],'total'), axis=1)
        chunk = chunk[ chunk['percentage'].notnull()  ]
        books_with_at_least_one_rating += len(chunk)
        books_with_30Percent_of_ratings_above_4 += len(chunk[chunk["percentage"] > 0.3]) # we interpret over as strictly over
        # print('processed rows:', processed_rows) # in total

    end = perf_counter()
    duration = end - start
    print('The process lasted approximately', '{:.2f}'.format(duration), "seconds.")

    print('We have found', books_with_30Percent_of_ratings_above_4, 'books with 30 Percent of ratings above 4')
    print('We have found', books_with_at_least_one_rating, 'books with at least one rating')
    print('The share of books with 30Percent of ratings above 4 is approximately: ', '{:.4f}'.format(books_with_30Percent_of_ratings_above_4 / books_with_at_least_one_rating))
    print('The process lasted approximately', '{:.2f}'.format(duration), "seconds.")

    return books_with_30Percent_of_ratings_above_4, books_with_at_least_one_rating


### RQ 7_2

def answer_rq_7_2(data_path):
    processed_rows = 0
    df = None
    flag = True
    # chunksize = 2*10**5
    # nrows = None # 3*10**5

    start = perf_counter()
    for chunk in pd.read_json( os.path.join( *data_path, "lighter_books" + ".json"), lines=True, nrows=nrows, chunksize=chunksize):
        processed_rows += len(chunk)
        if flag:
            df = chunk[['author_id','original_publication_date']] # ['author_id','original_publication_date']
            flag = False
        else:
            df = pd.concat([df, chunk[['author_id','original_publication_date']]  ], axis=0)
        # print('processed rows:', processed_rows)

    df_new = pd.DataFrame(df.groupby('author_id')['original_publication_date'].apply(functions.preprocessingRQ7_2))

    results = df_new[ (df_new['original_publication_date'] == True) | (df_new['original_publication_date'] == False)].groupby('original_publication_date')['original_publication_date'].apply(functions.listlen)
    # results
    print( '{:.4f}'.format(results.loc[True]/(results.loc[True]+results.loc[False])), 'is the probability that an author publishes a book within 2 years') # probability
    end = perf_counter()
    duration = end - start
    print('The process lasted approximately', '{:.2f}'.format(duration), "seconds.")

    return results


### RQ 7_3 and 7_4

def get_worst_book_ids_of_all_time(data_path):
    # assessing the books inside the list "The Worst Books of All Time".
    df_new = None # for the sake of RAM
    list_df = None
    # chunksize = 10**4
    # nrows = None # 2*10**4
    flag = True
    processed_rows = 0

    start = perf_counter()
    for chunk in pd.read_json(os.path.join( *data_path, 'list' + ".json") , lines=True, chunksize=chunksize, nrows=nrows):
        processed_rows += len(chunk)
        chunk = chunk[chunk['title']=="The Worst Books of All Time"]
        if flag:
            list_df = chunk
            flag = False
        else:
            list_df = pd.concat([list_df, chunk], axis=0)
        # print('processed rows:', processed_rows)

    worst_books_of_all_time = list_df[list_df['title']=="The Worst Books of All Time"]["books"]
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
    worst_book_ids_of_all_time_set = set()
    for book_id in worst_book_ids_of_all_time:
        try:
            worst_book_ids_of_all_time_set.add(int(book_id))
        except Exception as e:
            print(e)
            pass

    end = perf_counter()
    duration = end - start
    print('The process lasted approximately', '{:.2f}'.format(duration), "seconds.")
    return worst_book_ids_of_all_time_set


def get_contingency_table_for_rq_7_3(data_path, worst_book_ids_of_all_time):
    processed_rows = 0
    contingency_table = None
    flag = True

    # chunksize = 10**5
    # nrows = None # 2*10**5

    start = perf_counter()
    for chunk in pd.read_json(os.path.join( *data_path, "lighter_books" + ".json"), lines=True, chunksize = chunksize, nrows = nrows, dtype=None ):
        processed_rows += len(chunk)
        chunk = chunk[["id","num_pages"]] # to shrink needed memory
        chunk = chunk[(chunk["num_pages"]!="") & (chunk["id"]!="") ] # to exclude books without pages mentioned or invalid ids
        chunk.astype({'id':'int', 'num_pages':'int' })
        chunk = chunk[chunk["num_pages"]>0] # 0 or less pages is assumed to be a database error or a result of non-knowledge.
        chunk["gt700"] = chunk.apply( lambda row: row['num_pages'] > 700 , axis=1)
        chunk["oneOftheWorst"] = chunk.apply(lambda row : str(row['id']) in worst_book_ids_of_all_time, axis=1) # TODO: str() is necessary. It would be better if both were ints. But this raises errors.
        if flag:
            contingency_table = pd.crosstab(chunk['oneOftheWorst'], chunk['gt700'])
            flag = False
        else:
            contingency_table += pd.crosstab(chunk['oneOftheWorst'], chunk['gt700'])
        # print("processed rows:", processed_rows) # in total
        # print('current contingency table looks like:\n', contingency_table, "\n\n")
    end = perf_counter()
    duration = end - start
    print('The process lasted approximately', '{:.2f}'.format(duration), "seconds.")

    return contingency_table


def answer_rq_7_3(contingency_table):
    res = contingency_table.loc[True,True] / (contingency_table.loc[True,False]+contingency_table.loc[True,True])
    print("Now we can provide the answer to 3.")
    print("""by computing the ratio of the entry for "gt700 and oneOftheWorst", which is """, contingency_table.loc[True,True], ",")
    print("divided by the number of books with over 700 pages, which is", contingency_table.loc[True,False]+contingency_table.loc[True,True] ,".")
    print("This yields approximately:", '{:.4f}'.format(res))
    return res



def perform_fisher_test_and_interprete(contingency_table, alpha = 0.05):
    _, pval = fisher_exact(contingency_table)
    print('assuming a test-level of alpha =', alpha)
    strres = ""
    if pval <= alpha:
        strres = ", i.e. less than or equal to alpha, \nthere's significance for dependence."
    else:
        strres = ", i.e. strictly greater than alpha, \nthere's no significance for dependence."

    print("Since the pvalue is approximately", '{:.6f}'.format(pval), strres)
    return pval


### RQ 8

def answer_rq_8_1(data_path):
    books_df = None
    processed_rows = 0
    # chunksize = 2*10**5
    # nrows = None # 2*10**3
    flag = True

    start = perf_counter()
    for chunk in pd.read_json(os.path.join(*data_path, 'lighter_books' + '.json'), lines=True, chunksize=chunksize, nrows=nrows):
        processed_rows += len(chunk)
        chunk = chunk[['ratings_count','average_rating','num_pages']]
        chunk.loc[:,'ratings_count'] = pd.to_numeric(chunk.loc[:,'num_pages'], errors='coerce')
        chunk.loc[:,'average_rating'] = pd.to_numeric(chunk['average_rating'], errors='coerce')
        chunk.loc[:,'num_pages'] = pd.to_numeric(chunk['num_pages'], errors='coerce')
        chunk = chunk[ (~chunk['average_rating'].isna()) & (~chunk['num_pages'].isna()) & (~chunk['ratings_count'].isna())
                        & (chunk['num_pages']>0) & (0 <= chunk['average_rating']) & (chunk['average_rating'] <= 5) & (chunk['ratings_count']>0 )]
        chunk.loc[:,'average_rating'] = chunk['average_rating']
        # chunk['log(num_pages)'] = np.log10(chunk['num_pages'])
        chunk.loc[:,'log(num_pages)'] = np.log10(chunk['num_pages'])
        chunk.loc[:, 'log(num_pages)'] = chunk.apply( lambda row : np.log10(row['num_pages']) , axis=1)
        if flag:
            books_df = chunk
            flag = False
        else:
            books_df = pd.concat([books_df, chunk], axis=0)
        # print("processed rows:", processed_rows)
    # books_df
    end = perf_counter()
    duration = end - start
    print('The process lasted approximately', '{:.2f}'.format(duration), "seconds.")
    return books_df


def get_languages(data_path):
    processed_rows = 0
    # chunksize = 10**4
    # nrows = None
    languages_set = set()
    chunks = pd.read_json(os.path.join(*data_path, 'lighter_books' + '.json'), lines=True, chunksize=chunksize, nrows=nrows, dtype={'num_pages':'numeric', 'average_rating':'numeric'})
    for chunk in chunks:
        processed_rows += len(chunk)
        languages_set = languages_set.union(set(chunk['language']))
        # print(processed_rows)
    return languages_set

def get_eng_vs_non_eng(data_path, no_languages):
    books_df = None
    processed_rows = 0
    # chunksize = 10**5
    # nrows = None
    flag = True

    start = perf_counter()
    chunks = pd.read_json(os.path.join(*data_path, 'lighter_books' + '.json'), lines=True, chunksize=chunksize, nrows=nrows, dtype={'num_pages':'numeric', 'average_rating':'numeric'})
    for chunk in chunks:
        processed_rows += len(chunk)
        chunk = chunk[['ratings_count','average_rating','language']]
        chunk = chunk[ ~(chunk['language'].isin(no_languages)) ]
        chunk['ratings_count'] = pd.to_numeric(chunk['ratings_count'], errors='coerce')
        chunk['average_rating'] = pd.to_numeric(chunk['average_rating'], errors='coerce')
        chunk = chunk[ (~chunk['average_rating'].isna()) & (~chunk['ratings_count'].isna())
                        & (0 <= chunk['average_rating']) & (chunk['average_rating'] <= 5) & (chunk['ratings_count']>0 )]
        chunk['en'] = chunk['language'].apply(lambda entry : entry[:2] == "en" )
        if flag:
            books_df = chunk
            flag = False
        else:
            books_df = pd.concat([books_df, chunk], axis=0)
        # print(processed_rows)

    grouped_series = books_df.groupby('en')['average_rating'].apply(list)
    end = perf_counter()
    duration = end - start
    print('The process lasted approximately', '{:.2f}'.format(duration), "seconds.")
    return pd.DataFrame(grouped_series)


def perform_ks2s_test_and_interprete(sample1, sample2, alpha=0.05):
    _, pval  = ks_2samp(sample1, sample2)
    test_result = functions.eval_pval(pval, alpha, "a difference in the analyzed distributions")
    return pval

def get_data_on_lazyness(data_path):
    authors_df = None
    processed_rows = 0
    # chunksize = 10**5
    # nrows = None # 4*10**3
    flag = True

    start = perf_counter()
    chunks = pd.read_json(os.path.join(*data_path, 'lighter_authors' + '.json'), lines=True, chunksize=chunksize, nrows=nrows, dtype={'num_pages':'numeric', 'average_rating':'numeric'})
    for chunk in chunks:
        processed_rows += len(chunk)
        chunk = chunk[['text_reviews_count', 'fans_count']]
        chunk['text_reviews_count'] = pd.to_numeric(chunk['text_reviews_count'], errors='coerce')
        chunk['fans_count'] = pd.to_numeric(chunk['fans_count'], errors='coerce')
        chunk = chunk[ (~chunk['text_reviews_count'].isna()) & (~chunk['fans_count'].isna())
                        & (chunk['text_reviews_count']>=0 ) & (chunk['fans_count']>=0 )]
        chunk['8th-root(text_reviews_count)'] = (chunk['text_reviews_count'])**(1/8) # since we allow for 0-values, we use the root to lower the impact of outliers
        chunk['8th-root(fans_count)'] = (chunk['fans_count'])**(1/8)

        if flag:
            authors_df = chunk
            flag = False
        else:
            authors_df = pd.concat([authors_df, chunk], axis=0)
        # print(processed_rows)
    end = perf_counter()
    duration = end - start
    print('The process lasted approximately', '{:.2f}'.format(duration), "seconds.")
    return authors_df

def perform_lin_regression(model_set, col_name_1, col_name_2  ):
    model = LinearRegression()
    model.fit(model_set[[col_name_1]], model_set[col_name_2])
    score = model.score(model_set[[col_name_1]], model_set[col_name_2])
    return model, score


def interprete_lin_regression(model,score, eps=0.05):
    print('slope is', model.coef_[0])
    print('intercept is' ,model.intercept_)
    # print('residuals is', model.resid )
    print('R-squared-Coefficient is', score)
    if model.coef_[0] > eps:
        print("Since the slope is significantly greater than zero \nwe see that the more fans an author has the more text_reviews he has. \nHence we can refuse the stated claim that fans of authors with more fans would be lazier. ")
    elif model.coef_[0] < - eps:
        print("Since the slope is significantly lower than zero \nwe see that the more fans an author has the less text_reviews he has.  \nHence we can confirm the claim that fans of authors with more fans are lazier")
    else:
        print("Since the slope does not differ significantly from zero \nwe there's no evidence for a correlation between fans_count and text_reviews_count.")

    print('The R-squared-Coefficient is with approximately', '{:.2f}'.format(score))
    if score <= 0.1 :
        print("very low. Hence the model explains (nearly) nothing of the seen variance.")
    elif score <= 0.5:
        print("is low. Hence the model does explain only little of the seen variance.")
    elif score <= 0.7:
        print("is at least half. Hence the model does explain some of the seen variance.")
    elif score <= 0.9:
        print("is high. Hence the model does explain a big part of the seen variance.")
    else:
        print("is very high. Hence the model does explain (nearly) all of the seen variance.")

