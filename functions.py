import json
from datetime import datetime

### general functions
def get_from_config(data_dir):
    with open('config.json') as cfg:
        # print({k:"" for k in cfg}) # printing an empty sample-config
        cfg = json.load(cfg)
        if data_dir in cfg:
            return cfg[data_dir]
        else:
            return cfg


### functions for RQ 7

### functions for RQ 7.1
 
def parseToDict(mystr):
    """
    Takes a string and splits it first at "|", then at ":".
    Finally returns a dictionary whose items represent the substrings between "|"
    Where key and value are separated by ":".
    """
    myli = mystr.split("|")
    mydict = {}
    for entry in myli:
        k,v = entry.split(":")
        v = int(v)
        mydict[k] = v    
    return mydict

def checkValidityRQ7_1(my_input):
    """
    takes a dictionary,
    returns None if the input is not in  the correct format. otherwise returns the input.
    """
    total = 0
    for k,v in my_input.items():
        try:
          v = int(v)
          if v < 0 :
            return None
          total += v
        except Exception as e:
          print(e)
          return None
    my_input['total'] = total
    return my_input



def getRatio(my_dict, key_numerators, key_denominator):
    """
    Takes a dictionary, a list of keys called "key_numerators", and a "key_denominator".
    It returns a ratio.
    The ratio is computed by first summing up the values belonging to the keys deliverd in the "key_numerators"-lisit. 
    And then dividing by the value of the given key_denominator.
    If an ZeroDivisionError occurs None is returned.
    """
    if my_dict is None:
        return None
    else:
        try:
            return sum([my_dict[key_numerator] for key_numerator in key_numerators])/my_dict[key_denominator]
        except ZeroDivisionError as e:
            print(e)
            return None


### functions for RQ 7.2

def preprocessingRQ7_2(*args, **kwargs):
    """
    Takes a list of strings where each string is representing a date.
    Computes the differences in time of these dates with respect to the first entry of the list using the functions "myStringToDate" and "myDateToSeconds".
    Incompatible strings are dropped.
    Returns None if list-length became 0 during the process. Returns true if the average of the converted list is at most 2 years. Otherwise returns false.
    """
    return_value = None
    res = list(*args, **kwargs)
    # print(res)
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
    """
    Takes a string representing a date and a string representing a datetime-format.
    Returns the datestring as a datetime-object in the desired format.
    Returns None if the date_string and date_format do not fit.
    """
    res = None
    try:
        res = datetime.strptime(date_string, date_format)
    except ValueError as e:
        # print(e)
        pass
    return res

def myDateToSeconds(initial_date, current_date):
    """
    Takes an initial date and a current date, both as date-time-objects.
    Returns the total number of seconds from the initial to the current date.
    If an Exception occurs None is returned.
    """
    res = None
    try:
        res = current_date - initial_date
        res = res.total_seconds()
    except Exception as e:
        print(e)
    return res


def listlen(*args,**kwargs):
    return len(list(*args))



def eval_pval(pval, alpha, alternative):
    """
    Takes a p-value, a significance level alpha and the alternative formulated as an appropriate string
    Returns true iff there's significance for the alternative. Otherwise it returns false.
    Further prints a sentence describing the result to the console.
    """
    print('assuming a test-level of alpha =', alpha)
    strres = ""
    if pval <= alpha:
        strres = ", i.e. less than or equal to alpha, \nthere's significance for " + alternative + "."
    else:
        strres = ", i.e. strictly greater than alpha, \nthere's no significance for " + alternative + "."

    print("Since the pvalue is approximately", '{:.6f}'.format(pval), strres)
    return pval <= alpha
