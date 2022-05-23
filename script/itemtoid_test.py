import requests
import csv
import re

from itemtoid import prep_query

"""
qdata = {
        "fname": fname,
        "lname": lname,
        "nobname_sts": nobname_sts,
        "status": sts_title,
        "dates": dates,
        "function": function,
        "rebuilt": rebuilt,
        "abv": abv
    }
"""


# ================= TESTING FUNCTIONS ================= #
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


def test_query(qdict, test_base, test_rbfalse, nrow):
    """
    get the wikipedia id from a full text query and launch the http get query. using the results of the
    request, build a dictionary containing success statistics
    :param qdict: a dictionary of query data created with prep_query()
    :param test_base: a dictionary to store the query results when rebuilt is True and False
    :param test_rbfalse: a dictionary to store the query results using only non-rebuilt names (rebuilt = False)
    :param nrow: the index of the current nametable_* row
    :return:
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

    # build different srsearch params and launch queries
    qspecs = ["pass", "nobname_sts", "status", "dates", "function"]  # keys of qdict for the different queries to run
    n = 0  # count number of iterationd
    for q in qspecs:
        if n == 0:
            params["srsearch"] = re.sub("\s+", " ", f"{qdict['fname']} {qdict['lname']}")
        else:
            params["srsearch"] = re.sub("\s+", " ", f"{qdict['fname']} {qdict['lname']} {qdict[q]}")
        print(params["srsearch"])
        r = requests.get(url, params=params, headers=headers)
        # print(r.text)
        js = r.json()
        try:
            r_id = js["query"]["search"][0]["title"]
        except IndexError:
            r_id = ""

        if r_id == read_id(nrow):
            print(r_id, read_id(nrow))
        else:
            print("NO HAY BANDAS")
        n += 1
    print("________________________________")

    return test_base, test_rbfalse


# ================= DEFINE QUERIED DATA ================= #
def itemtoid_test():
    """
    launch the query on all entries of nametable_test_noid.tsv to verify the precision
    :return:
    """
    with open("tables/nametable_test_noid.tsv", mode="r", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        nrow = 0
        prev = {}  # dictionary to store the value of qdict at the previous iteration, in case tei:name == "Le même"

        # dictionaries to store test results. each entry in the dictionary has for values "success" (percentage of
        # correct wikidata ids) and "total" (total number of entries for which we have data to be tested :
        # number of entries with nobname_sts...)
        test_base = {
            "fl": {"success": (0, 0), "total": 0},  # base query : the data from fname + lname in qdict; we don't care
            #                                     wether rebuilt is True/False
            "fl nobname_sts": {"success": (0, 0), "total": 0},  # fname + lname + nobname
            "fl status": {"success": (0, 0), "total": 0},  # fname + lname + sts
            "fl dates": {"success": (0, 0), "total": 0},  # fname + lname + dates
            "fl function": {"success": (0, 0), "total": 0},  # fname + lname + function
        }  # dictionary to store the data of tests. here, fname can be rebuilt or not
        test_rbfalse = {
            "fl": {"success": (0, 0), "total": 0},  # base query : the data from fname + lname in qdict; here,
            # only first names that have not been rebuilt using namebuild() are kept
            "fl nobname_sts": {"success": (0, 0), "total": 0},  # fname + lname + nobname
            "fl status": {"success": (0, 0), "total": 0},  # fname + lname + sts
            "fl dates": {"success": (0, 0), "total": 0},  # fname + lname + dates
            "fl function": {"success": (0, 0), "total": 0},  # fname + lname + function
        }

        # launch the queries
        for row in reader:
            in_data = [row[2], row[3]]  # input data on which to launch a query
            qdict, prev = prep_query(in_data, prev)
            test_base, test_rbfalse = test_query(qdict, test_base, test_rbfalse, nrow)
            nrow += 1



# ================= LAUNCH THE SCRIPT ================= #
if __name__ == "__main__":
    print(itemtoid_test())
