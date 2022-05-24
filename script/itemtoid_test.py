import requests
from tqdm import tqdm
import json
import csv
import re
import os

from itemtoid import prep_query


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


def test_query(qdict, test_base, test_rebuilt, nrow):
    """
    get the wikipedia id from a full text query and launch the http get query. using the results of the
    request, build a dictionary containing success statistics
    :param qdict: a dictionary of query data created with prep_query()
    :param test_base: a dictionary to store the query results when rebuilt is True and False
    :param test_rebuilt: a dictionary to store the query results using only non-rebuilt names (rebuilt = False)
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
            params["srsearch"] = re.sub("\s+", " ", name)
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
        trows = sum(1 for row in reader)  # total number of rows
        fh.seek(0)

        # launch the queries
        for row in tqdm(reader, desc="retrieving IDs from the wikidata API", total=trows):
            in_data = [row[2], row[3]]  # input data on which to launch a query
            qdict, prev = prep_query(in_data, prev)
            test_base, test_rebuilt = test_query(qdict, test_base, test_rebuilt, nrow)
            nrow += 1

    # build the test dictionaries data
    for k, v in test_base.items():
        if v["success"] != 0:
            test_base[k]["success"] = round((v["success"] / v["total"] * 100), 2)
        else:
            test_base[k]["success"] = 0
    for k, v in test_rebuilt.items():
        if v["success"] != 0:
            test_rebuilt[k]["success"] = round((v["success"] / v["total"] * 100), 2)
        else:
            test_rebuilt[k]["success"] = 0

    # build output json file and write the file
    outdict = {"base query": test_base, "no rebuilt names": test_rebuilt}
    if not os.path.isdir(os.path.join(os.getcwd(), "test_out")):
        os.makedirs(os.path.join(os.getcwd(), "test_out"))
    with open(f"{os.path.join(os.getcwd(), 'test_out')}/itemtoid_test_result.json", mode="w") as out:
        json.dump(outdict, out, indent=4)
    print("tests finished !")

    return None


# ================= LAUNCH THE SCRIPT ================= #
if __name__ == "__main__":
    itemtoid_test()
