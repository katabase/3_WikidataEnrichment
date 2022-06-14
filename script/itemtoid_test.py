from tqdm import tqdm
import requests
import random
import string
import json
import time
import csv
import re
import os

from itemtoid import launch_query
from prepare_query import prep_query


# ================= TESTING FUNCTIONS ================= #
def makedummy():
    """
    build dummy, a json to check what's quicker:
    - run all queries, no matter whether if they've aldready been ran
    - check in a large json file if the query has aldready been ran and get the result from there
    the structure of the dummy json is similar to the structure of the json file to be used with the final dataset
    :return: None
    """
    dummy = {}
    for i in range(10000):
        # about 30% of wikidata ids are certain ; add dummy booleans in that proportion to dummy
        rand = random.randrange(1, 10)
        if rand >= 7:
            dummy_cert = True
        else:
            dummy_cert = False
        dummy_key = "".join(random.choices(string.ascii_lowercase, k=15))
        dummy_w_id = f"Q{''.join(random.choices(string.digits, k=7))}"
        dummy_title = "".join(random.choices(string.digits, k=10))
        dummy_snippet = "".join(random.choices(string.digits, k=20))
        dummy[dummy_key] = [dummy_w_id, dummy_title, dummy_snippet, dummy_cert]
    with open("tables/dummy.json", mode="w") as out:
        json.dump(dummy, out, indent=4)
    return None


def read_id(i):
    """
    access the correct wikidata id
    :param i: the row for which we want to retrieve an id
    :return: the propert wikidata id
    """
    with open("tables/nametable_test_withid.tsv", mode="r") as fh:
        reader = csv.reader(fh, delimiter="\t")
        w_id = [row for idx, row in enumerate(reader) if idx == i][0][1]
    return w_id


def test_isolate(qdict, test_base, test_rebuilt, nrow):
    """
    get the wikipedia id from a full text query and launch the http get query. using the results of the
    request, build a dictionary containing success statistics
    :param qdict: a dictionary of query data created with prep_query()
    :param test_base: a dictionary to store the query results when rebuilt is True and False
    :param test_rebuilt: a dictionary to store the query results using only non-rebuilt names (rebuilt = False)
    :param nrow: the index of the current nametable_* row
    :return: test_final (dictionary with stats for the final query algorithm) and runtime (float of the runtime of
             the query algorithm)
    """
    # build query url
    url = "https://www.wikidata.org/w/api.php"
    # define headers : set a user-agent, as per the api's etiquette (https://www.mediawiki.org/wiki/API:Etiquette);
    # gzip encoding makes requests faster
    headers = {
        "User-Agent": "katabot/1.0 (https://katabase.huma-num.fr/) python/requests/2.27.1",
        "Accept-Encoding": "gzip"
    }
    # base parameters, to which we'll add srsearch with the specific query string
    params = {
        "action": "query",
        "list": "search",
        "srlimit": 1,
        "format": "json"
    }
    
    # first query : name only. 2 queries are launched:
    # - one with the first name and last name no matter what, no matter whether rebuilt is True/False
    # - one with the last name and first name only if it has not been rebuilt
    idlist = []  # list to store the 2 wikipedia ids retrieved from the queries

    # build namelist: list to store the 2 names for the 2 queries
    # (fname + lname and lname + fname only if rebuilt is False)
    namelist = [f"{qdict['fname']} {qdict['lname']}"]
    if qdict["rebuilt"] is True:
        namelist.append(qdict["lname"])
    else:
        namelist.append(f"{qdict['fname']} {qdict['lname']}")

    # loop over the 2 names, launch the 2 queries and get the r_ids
    for name in namelist:
        if not re.match("^\s*$", name):
            params["srsearch"] = re.sub(r"\s+", " ", name)
            r = requests.get(url, params=params, headers=headers)
            js = r.json()
            try:
                idlist.append(js["query"]["search"][0]["title"])
            except IndexError:
                idlist.append("")
        else:
            idlist.append("")

    # add statistical data indicating success and number of times name only has been
    # queried for the whole dataset
    if idlist[0] == read_id(nrow):
        test_base["fname lname"]["success"] += 1
        test_base["fname lname"]["total"] += 1
    else:
        test_base["fname lname"]["total"] += 1
    if idlist[1] == read_id(nrow):
        test_rebuilt["fname lname"]["success"] += 1
        test_rebuilt["fname lname"]["total"] += 1
    else:
        test_rebuilt["fname lname"]["total"] += 1

    # build different srsearch params and launch queries
    qspecs = ["nobname_sts", "status", "dates", "function"]  # keys of qdict for the different queries to run
    for q in qspecs:
        # if there's info for the queried data in qdict (if there's a nobility title, dates...)
        if not re.match(r"^\s*$", qdict[q]):

            # build the same two queries as before, depending on the value of qdict["rebuilt"]
            idlist = []
            # for each name, retrieve an id and append it to the list
            for name in namelist:
                qstr = re.sub(r"\s+", " ", f"{name} {qdict[q]}")
                if not re.match(r"^\s*$", qstr):
                    # launch query and extract the wikidata id
                    params["srsearch"] = qstr
                    r = requests.get(url, params=params, headers=headers)
                    js = r.json()
                    # print(r.text)
                    try:
                        idlist.append(js["query"]["search"][0]["title"])
                    except IndexError:
                        idlist.append("")  # if there is no result to the query
                else:
                    idlist.append("")  # if there is no data to query, don't launch the query

            # add statistical data indicating success and number of times
            # data (dates, nobility titles...) has been queried and the number of times we got the right anwser
            if idlist[0] == read_id(nrow):
                test_base[f"fname lname {q}"]["success"] += 1
                test_base[f"fname lname {q}"]["total"] += 1
            else:
                test_base[f"fname lname {q}"]["total"] += 1
            if idlist[1] == read_id(nrow):
                test_rebuilt[f"fname lname {q}"]["success"] += 1
                test_rebuilt[f"fname lname {q}"]["total"] += 1
            else:
                test_rebuilt[f"fname lname {q}"]["total"] += 1

    return test_base, test_rebuilt


def test_algorithm(fetch, nloop=1):
    """
    test the final algorithm
    :param fetch: boolean indicating wether to fetch if a query has aldready
    been ran in a dummy json file or if all the queries must be ran.
    see itemtoid_test() for the json structure
    :param nloop: number of times to run the algorithm on all entries of the test dataset
    :return: test_final, dictionary of data on the final test
    """
    with open("tables/nametable_test_withid.tsv", mode="r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        total_ids = 0  # total number of wikidata ids in the test dataset
        total_silence = 0  # total number of empty wikidata ids in the test dataset
        # calculate the total number of wikidata ids found
        for row in reader:
            if row[1] != "":
                total_ids += 1
            else:
                total_silence += 1

    with open("tables/nametable_test_noid.tsv", mode="r", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        runtime = []
        for i in range(nloop):
            total = 0  # total number of entries queried
            test_result = 0  # total of wikidata ids found
            test_silence = 0  # total of wikidata ids not found
            true_result = 0  # total of correct wikidata ids found
            false_result = 0  # total of false wikidata ids found
            true_silence = 0  # total of wikidata ids that haven't been found where it's not a mistake
            false_silence = 0  # total of wikidata ids that haven't been found when an id should have been found
            cert_positive = 0  # total of certain positives
            cert_false_positive = 0  # total of certain positives returned by launch_query() that turn out to be false
            prev = {}
            test_final = {"success": 0, "f1_result": 0, "f1_silence": 0, "total": 0}
            trows = sum(1 for row in reader)  # total number of rows
            nrow = 0
            fh.seek(0)

            if fetch is True:
                makedummy()  # build a dummy json file
            start = time.time()  # to count the execution time

            for row in tqdm(reader, desc="retrieving IDs from the wikidata API", total=trows):
                in_data = [row[2], row[3]]  # input data on which to launch a query
                qdict, prev = prep_query(in_data, prev)
                out = launch_query(qdict, {"test": True, "fetch": fetch})

                # get the wikidata id and certitude level and build stats
                w_id = out[0]
                cert = out[3]
                if w_id == read_id(nrow):  # if the result is correct
                    test_final["success"] += 1
                    if w_id != "":
                        true_result += 1
                    else:
                        true_silence += 1
                else:
                    if w_id != "":
                        false_result += 1
                    else:
                        false_silence += 1
                if w_id != "":
                    if cert is True:
                        cert_positive += 1
                    if cert is True and w_id != read_id(nrow):
                        cert_false_positive += 1
                    test_result += 1
                else:
                    test_silence += 1
                total += 1
                nrow += 1

            runtime.append(time.time() - start)
            if fetch is True:
                os.remove("tables/dummy.json")  # delete the dummy file

    # crunch statistical data:
    # - precision and recall for both true positives (a correct wikidata id has been found)
    #   and true negatives (a wikidata id has not been found, but no wikidata id is present in reader)
    #   (precision_result, precision_silence, recall_result, recall_silence)
    # - f1 score for true positives and negatives (f1_result and f1_silence)
    # - proportion of successful queries (success)
    # - proportion of queries that have returned an id and that are marked as "certain" (where cert is True)
    #   (certitude_result)
    # - proportion of queries that have returned a result and are marked as "certain" but that returned,
    #   in fact, a wrong result (certitude_false_positive)
    # - number of wikidata ids found (found_ids) and not found (no_id_found)
    precision_result = true_result / test_result
    precision_silence = true_silence / test_silence
    recall_result = true_result / total_ids
    recall_silence = true_silence / total_silence
    test_final["success"] = round((test_final["success"] / total), 3)
    test_final["f1_result"] = round(2 * (precision_result * recall_result) / (precision_result + recall_result), 3)
    if precision_silence != 0 or recall_silence != 0:
        test_final["f1_silence"] = \
            round(2 * (precision_silence * recall_silence) / (precision_silence + recall_silence), 3)
    else:
        test_final["f1_silence"] = 0.0
    test_final["precision_result"] = round(precision_result, 3)
    test_final["recall_result"] = round(recall_result, 3)
    test_final["precision_silence"] = round(precision_silence, 3)
    test_final["recall_silence"] = round(recall_silence, 3)
    test_final["certitude"] = round(cert_positive / total, 3)
    test_final["certitude_false"] = round(cert_false_positive / total, 3)
    test_final["total"] = total
    test_final["found_ids"] = test_result
    test_final["no_id_found"] = test_silence

    # calculate the runtime and return the dictionary
    runtime = sum(runtime) / len(runtime)  # average runtime over the 3 iterations
    print(test_final)
    return test_final, runtime


# ================= BUILDING QUERIES ================= #
def count_empty():
    """
    count number of empty cells in test and complete dictionary to ensure that
    the test dataset represents well the whole dataset
    :return: statdict
    """
    statdict = {"test": {"percent empty": 0, "empty rows": 0, "total rows": 0},
                "real": {"percent empty": 0, "empty rows": 0, "total rows": 0}}
    with open("tables/nametable_test_noid.tsv", mode="r", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        for r in reader:
            if re.match(r"^\s*$", r[3]):
                statdict["test"]["empty rows"] += 1
            statdict["test"]["total rows"] += 1
    with open("tables/nametable_in.tsv", mode="r", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        for r in reader:
            if re.match(r"^\s*$", r[3]):
                statdict["real"]["empty rows"] += 1
            statdict["real"]["total rows"] += 1
    for k in statdict.keys():
        statdict[k]["percent empty"] = round(statdict[k]["empty rows"] / statdict[k]["total rows"] * 100, 2)
    descs_to_add = round(statdict["test"]["empty rows"] -
                         (statdict["test"]["total rows"] * (statdict["real"]["percent empty"] / 100)), 1)
    print(statdict)
    print(f"Number of descriptions to add to the test dataset: {descs_to_add}")
    return statdict


def itemtoid_test():
    """
    run tests on all isolated parameters and on the final algorithm for a test dataset of 200
    entires. the test dataset has the same proportion of tei:traits as the main data set and
    roughly the same kind of entries in the same proportions (persons, events, places...).
    store the output to out/itemtoid_test_out.json. json structure:
    {
      base_query: {
        query parameters: {
          success: # the number of successful queries
          total: # the total number of queries ran with those parameters
        }
      }  # the results for the query with rebuilt and not rebuilt names
      no_rebuilt_names: {
        query parameters: {
          success: # the proportion of successful queries (0: none, 1: 100%)
          total: # the total number of queries ran with those parameters
        }
      }
      "final_algorithm": {
        "success":  # proportion of queries where the good result has been found (0: none, 1: 100%)
        "f1_result":  # f1 score for the when an id has been found (0: none, 1: 100%)
        "f1_silence": # f1 score for the when no id has been found (0: none, 1: 100%)
        "total": # total number of items queried
        "precision_result":  # precision for when a result has been found (0: none, 1: 100%)
        "recall_result": # recall for when a result has been found (0: none, 1: 100%)
        "precision_silence": # precision for when a result no been found (0: none, 1: 100%)
        "recall_silence":  # recall for when no result has been found (0: none, 1: 100%)
        "certitude": # proportion of items where the result is marked "certain" (0: none, 1: 100%)
        "certitude_false":  # proportion of "certain" results that are not correct (false positives) (0: none, 1: 100%)
        "found_ids":  # total number of found ids
        "no_id_found":  # total number of ids that haven't been found
        "runtime_fetch":  # average runtime for the 200 items when we fetch aldready ran queries in a json
        "runtime_nofetch":  # average runtime for the 200 items when we run every single query without
                              checking if they've ben ran before
      }
    }
    proportions are relative to the total number of items. a proportion of 0 means 0%, a proportion
    of 1 means a 100 %

    about the precision, recall and the f1:
    https://fr.wikipedia.org/wiki/Pr%C3%A9cision_et_rappel
    https://scikit-learn.org/stable/modules/generated/sklearn.metrics.f1_score.html
    :return: None
    """
    with open("tables/nametable_test_withid.tsv", mode="r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        total_ids = 0  # total number of wikidata ids in the test dataset
        total_silence = 0  # total number of empty wikidata ids in the test dataset
        # calculate the total number of wikidata ids found
        for row in reader:
            if row[1] != "":
                total_ids += 1
            else:
                total_silence += 1

    # running the tests of the final algorithm
    # launch 2 different queries 3 times: first, with "fetch" = True; then, with "fetch" = False
    # (the queries are ran 3 times to see which one is quicker)
    # to determine which is quicker:
    # - to run the queries everytime, no matter if the query has aldready been ran
    # - to save new queries and their result to a large json file and to browse the
    #   json file for a result each time to get a result
    print("~ tests for the final algorithm started ! ~")
    test_final, runtime_nofetch = test_algorithm(fetch=False, nloop=3)
    test_final, runtime_fetch = test_algorithm(fetch=True, nloop=3)
    test_final["runtime_fetch"] = f"{runtime_fetch} seconds"
    test_final["runtime_nofetch"] = f"{runtime_nofetch} seconds"
    print("~ tests for the final algorithm finished ! ~")

    # running tests for isolate parameters
    print("~ tests for isolate parameters started ! ~")
    with open("tables/nametable_test_noid.tsv", mode="r", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        nrow = 0
        prev = {}  # dictionary to store the value of qdict at the previous iteration, in case tei:name == "Le même"

        # dictionaries to store test results for isolate parameters (name, date...).
        # each entry in the dictionary has for values "success" (percentage of correct wikidata ids)
        # and "total" (total number of entries for which we have data to be tested :
        # number of entries with nobname_sts...)
        test_base = {
            "fname lname": {"success": 0, "total": 0},  # base query : the data from fname + lname in qdict; we don't care
            #                                     wether rebuilt is True/False
            "fname lname nobname_sts": {"success": 0, "total": 0},  # fname + lname + nobname
            "fname lname status": {"success": 0, "total": 0},  # fname + lname + sts
            "fname lname dates": {"success": 0, "total": 0},  # fname + lname + dates
            "fname lname function": {"success": 0, "total": 0},  # fname + lname + function
        }  # dictionary to store the data of tests. here, fname can be rebuilt or not
        test_rebuilt = {
            "fname lname": {"success": 0, "total": 0},  # base query : the data from fname + lname in qdict; here,
            # only first names that have not been rebuilt using namebuild() are kept
            "fname lname nobname_sts": {"success": 0, "total": 0},  # fname + lname + nobname
            "fname lname status": {"success": 0, "total": 0},  # fname + lname + sts
            "fname lname dates": {"success": 0, "total": 0},  # fname + lname + dates
            "fname lname function": {"success": 0, "total": 0},  # fname + lname + function
        }

        # launch the queries for test_isolate
        trows = sum(1 for row in reader)  # total number of rows
        fh.seek(0)
        for row in tqdm(reader, desc="retrieving IDs from the wikidata API", total=trows):
            in_data = [row[2], row[3]]  # input data on which to launch a query
            qdict, prev = prep_query(in_data, prev)
            test_base, test_rebuilt = test_isolate(qdict, test_base, test_rebuilt, nrow)
            nrow += 1
        print("~ tests for isolate parameters finished ! ~")

    # build the test dictionaries data : isolate the effect of a single key-value pair of
    # qdict in obtaining the correct result
    for k, v in test_base.items():
        if v["success"] != 0:
            test_base[k]["success"] = round((v["success"] / v["total"]), 3)  # calculate a proportion
        else:
            test_base[k]["success"] = 0  # avoid divisions by 0
    for k, v in test_rebuilt.items():
        if v["success"] != 0:
            test_rebuilt[k]["success"] = round((v["success"] / v["total"]), 3)  # calculate a proportion
        else:
            test_rebuilt[k]["success"] = 0

    print(test_base)
    print(test_rebuilt)

    # save the tests result in the out directory
    outdict = {"base_query": test_base, "no_rebuilt_names": test_rebuilt, "final_algorithm": test_final}
    if not os.path.isdir("out"):
        os.makedirs("out")
    with open(os.path.join(os.getcwd(), "out", "itemtoid_test_out.json"), mode="w") as out:
        json.dump(outdict, out, indent=4)

    print(outdict)
    print("~ tests finished ! ~")
    return None


# ================= LAUNCH THE SCRIPT ================= #
if __name__ == "__main__":
    itemtoid_test()
