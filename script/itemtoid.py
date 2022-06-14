from pathlib import Path
from tqdm import tqdm
import traceback
import requests
import json
import csv
import sys
import re
import os

from rgx import names
from prepare_query import prep_query


# get a wikidata id from a full text query
# wikidata api documentation : https://www.mediawiki.org/wiki/API:Etiquette


# ================= MAKE AND LAUNCH THE QUERIES ================= #
def request(qstr, qdict):
    """
    run a full text search on wikidata, determine the degree of certitude
    of the obtained result (based on the values of qdict used to build
    the query string) and return the result
    :param qstr:
    :param qdict:
    :return: out, a list of the results : ["wikidata id", "page title", "page snippet", "certitude"]
    """
    paramcount = 0  # count number of parameters to determine if cert is True
    qdate = False  # boolean indicating if there is a date in a successful query; True if there's a date
    cert = False
    # build query url, define parameters and define headers :
    # - set a user-agent, as per the api's etiquette (https://www.mediawiki.org/wiki/API:Etiquette);
    # - accept gzip encoding (makes requests faster)
    url = "https://www.wikidata.org/w/api.php"
    headers = {
        "User-Agent": "katabot/1.0 (https://katabase.huma-num.fr/) python/requests/2.27.1",
        "Accept-Encoding": "gzip"
    }
    params = {
        "action": "query",
        "list": "search",
        "srsearch": qstr.strip(),
        "srlimit": 1,
        "srprop": "title|titlesnippet|snippet",
        "format": "json"
    }
    # print(qstr)
    # launch query and return out, a list of the wikidata ID, the wikidata title page, a snippet of the page
    r = requests.get(url, params=params, headers=headers)
    js = r.json()
    try:
        out = [
            js["query"]["search"][0]["title"],
            striptag(js["query"]["search"][0]["titlesnippet"]),
            js["query"]["search"][0]["snippet"]
               ]

    # if there's a mistake, at a list with 3 empty elements (we add empty elements cause
    # we rely on the index of elements in the list for the rest of the algorithm)
    except IndexError:
        out = ["", "", ""]
    except KeyError:
        out = ["", "", ""]
    # assert the certainty of the result : whether the id found is "certain" or not,
    # from the values of qdict used to build a query
    for k, v in qdict.items():
        # if there's at least 1 date (of dates) in the query string, qdate is True and
        # paramcount add 1 to paramcount
        if k == "dates" and not re.match(r"^\s*$", v):
            if qdict["dates"].split()[0] in qstr \
                    or qdict["dates"].split()[-1] in qstr:
                qdate = True
                paramcount += 1
        # if the first name hasn't been rebuilt and a result has been found, that's an extra
        # point !
        elif k == "rebuilt" and v is True and not re.match(r"^\s*$", qdict["fname"]):
            paramcount += 1
        # if other parameters are in qstr
        elif k != "rebuilt" and not re.match(r"^\s*$", v) and v.lower().strip() in qstr:
            paramcount += 1
    # if paramcount >= 5 or qdate is True: certitude == 0.305, certitude_false == 0.08
    # if paramcount >= 4 or qdate is True: certitude == 0.335, certitude_false == 0.09
    # if paramcount >= 4: certitude == 0.21, certutude_false == 0.05
    # if paramcount >= 3 and qdate is True: certitude == 0.255, certitude_false: 0.06
    if paramcount >= 4 or qdate is True:  # or qdate is True:
        cert = True

    out.append(cert)
    return out


def makerequest(qstr, qdict, config=None):
    """
    configure the request and launch it
    :param qstr: the query string
    :param qdict: the dictionary from which the query is built (to check wether dates
    :param config: a dictionnary indicating how to launch the query:
                   - config["test"] indicates if it's a test or the true query, to choose which json to load
                   - config["fetch"] indicates if a json should be loaded at all or not
    are in the query string)
    :return:
    """
    # remove duplicate words from qstr
    qstr = qstr.lower().split()
    qstr_ddp = re.sub(r"\s+", " ", " ".join(sorted(set(qstr), key=qstr.index)))  # ddp = de-duplicated

    # check if the query has aldready been done; if so, get the result from the json
    if config is None:
        config = {"test": False, "fetch": True}

    # if fetch is True, try to fetch the query in the queried json. if the query has not aldready been run,
    # launch the query on the wikidata API  and write the output to queried.
    # if fetch is False, run the query directly
    if config["fetch"] is True:
        if config["test"] is True:
            with open("tables/dummy.json", mode="r+") as fh:
                queried = json.load(fh)
                if qstr_ddp in queried.keys():
                    out = queried[qstr_ddp]
                else:
                    out = request(qstr_ddp, qdict)
                    queried[qstr_ddp] = out
                    fh.seek(0)
                    json.dump(queried, fh, indent=4)
        else:
            if not os.path.isfile("tables/queried.json"):
                Path("tables/queried.json").touch()  # create file
            with open("tables/queried.json", mode="r+") as fh:
                if not os.stat("tables/queried.json").st_size == 0:
                    # if the file is not empty, normal functionning: read the json
                    # to see if the query has aldready been ran ; return the result
                    queried = json.load(fh)
                    if qstr_ddp in queried.keys():
                        out = queried[qstr_ddp]
                    else:
                        out = request(qstr_ddp, qdict)
                        queried[qstr_ddp] = out
                        fh.seek(0)
                        json.dump(queried, fh, indent=4)
                else:
                    # if the file is empty (on the first request), write the request's result
                    # to the queried json file
                    queried = {}
                    out = request(qstr_ddp, qdict)
                    queried[qstr_ddp] = out
                    fh.seek(0)
                    json.dump(queried, fh, indent=4)
    else:
        out = request(qstr_ddp, qdict)
    return out


def relaunch_query(qstr, qdict, avail, config=None):
    """
    relaunch a query for launch_query() if it fails : remove one parameter at
    a time and relaunch the query
    :param qstr: the query string
    :param qdict: the dictionary from which the query string is being build
    :param avail: the list of available query parameters (non empty keys of qdict that aren't fname, lname or rebuilt)
    :param config: a dictionary with 2 keys ("test" + "fetch") to pass to makerequest(). see makerequest() for details
    :return:
    """
    out = ["", "", "", False]
    # if there are two dates (birth + death), first remove the first date and launch the query;
    # if there's no result, remove the second date and relaunch the query
    if len(list(qdict["dates"].split())) == 2:
        dates = list(qdict["dates"].split())
        qstr_dates = qstr.replace(dates[0], "")
        out = makerequest(qstr_dates, qdict, config)
        if out[0] == "":
            qstr_dates = qstr.replace(dates[1], "")
            out = makerequest(qstr_dates, qdict, config)

    # if there's no result after running the query with one less date,
    # relaunch the query in a substractive manner (by removing one available query parameter (value of qdict))
    if out[0] == "":
        for a in avail:
            qstr = qstr.replace(qdict[a], "")
            out = makerequest(qstr, qdict, config)
            if len(out) != "":
                break  # info has been found. stop looping
            else:
                qstr = f"{qstr} {qdict[a]}"  # add the removed parameter
                #                              before looping on another param
    return out


def launch_query(qdict, config=None):
    """
    main query algorithm.
    build a wikidata full text search from a dictionary of structured ; depending on the
    result of a query (if a wikidata id is found or not), relaunch the query with different parameters.
    (see prep_query() for the preparation of qdict, a structured dictionary made from the tei:trait
    and tei:name).
    :param qdict: the dictionary from which the query string is being build (see prep_query())
    :param config: a dictionary with 2 keys ("test" + "fetch") to pass to makerequest(). see makerequest() for details
    :return:
    """
    if config is None:
        config = {"test": False, "fetch": True}
    avail = []  # dictionary indicating which infos are available in qdict

    # build a list of available query data(aka, non - empty values of qdict)
    # for some reason that i don't understand, the first name may not be fully rebuilt
    # by namebuild; check if that's the case and rebuild it
    for k, v in qdict.items():
        if k == "fname" and not re.match(r"^\s*$", v):
            for abv, full in names.items():
                if re.search(f"(^|-|\s){abv}(\s|\.|-|$)", v):
                    v = v.replace(abv, full)
                    qdict["fname"] = v
        elif not re.search("(fname|lname|rebuilt)", k) and not re.match(r"^\s*$", v):
            avail.append(k)  # add non-empty query parameters to avail

    # build query string. the first query is made with all available data
    qstr = f"{qdict['fname']} {qdict['lname']} {qdict['status']} \
            {qdict['nobname_sts']} {qdict['dates']} {qdict['function']}".lower()

    # launch query if there is info on which to be queried
    if not re.match(r"^\s*$", qstr):
        out = makerequest(qstr, qdict, config)

        # extra behaviour if there's no result : delete first name if it was rebuilt,
        # remove one of qdict parameters per query (except for lname and fname)
        if out[0] == "":
            # if there's a nobility name, try to remove the fname or lname or both (conditions 1, 2, 3)
            # if all else fails, relaunch_query without fname or lname
            if qdict["nobname_sts"] != "":
                if qdict["fname"] != "":
                    qstr = qstr.replace(qdict["fname"], "")
                    out = makerequest(qstr, qdict, config)
                if out[0] == "" and qdict["lname"] != "":
                    qstr = qstr.replace(qdict["lname"], "")
                    if not re.match(r"^\s*?", qdict["fname"]):
                        qstr = qstr + f" {qdict['fname']} "
                    out = makerequest(qstr, qdict, config)
                if out[0] == "" and qdict["fname"] != "" and qdict["lname"] != "":
                    qstr = qstr.replace(qdict["fname"], "")
                    qstr = qstr.replace(qdict["lname"], "")
                    out = makerequest(qstr, qdict, config)
                if out[0] == "":
                    out = relaunch_query(qstr, qdict,  avail, config)

            # remove a parameter and relaunch the query
            elif len(avail) >= 1:
                out = relaunch_query(qstr, qdict, avail, config)

            # relaunch the query without the rebuilt name
            elif qdict["rebuilt"] is True:
                qstr = f"{qdict['fname']} {qdict['lname']} {qdict['status']} \
                        {qdict['nobname_sts']} {qdict['dates']} {qdict['function']}".lower()
                qstr = qstr.replace(qdict["fname"], "")
                out = makerequest(qstr, qdict, config)
                if out[0] == "" and len(avail) >= 1:
                    out = relaunch_query(qstr, qdict, avail, config)

            # if all else fails, get the original query string back ;
            # remove the first name (a french version of the name  can be in the catalogs,
            # while a foreign version only is on wikidata) and relaunch the query
            if out[0] == "" and not re.search(r"^\s*$", qdict["fname"]):
                qstr = qstr.replace(qdict["fname"], "")
                out = makerequest(qstr, qdict, config)
                if out[0] == "" and len(avail) >= 1:
                    out = relaunch_query(qstr, qdict, avail, config)

    # if there's no data to be queried
    else:
        out = ["", "", "", False]

    return out


def itemtoid(config=None):
    """
    launch the query on all entries of nametable_in.tsv. a nametable_out.tsv output
    file is created in out/, even if it's not used in tests (which it's not supposed to be).
    :return:
    """
    if config is None:
        config = {"test": False, "fetch": True}
    with open("out/nametable_out.tsv", mode="a+", encoding="utf-8") as f_out:
        if config["test"] is True:
            f_in = open("tables/nametable_test_noid.tsv", mode="r+", encoding="utf-8")
        else:
            f_in = open("tables/nametable_in.tsv", mode="r+", encoding="utf-8")

        in_reader = csv.reader(f_in, delimiter="\t")
        out_writer = csv.writer(f_out, delimiter="\t")
        out_reader = csv.reader(f_out, delimiter="\t")
        queried = []
        prev = {}

        # write the column headers if output is empty
        if os.stat("out/nametable_out.tsv").st_size == 0:
            out_writer.writerow(["tei:xml_id", "wd:id", "tei:name", "wd:name",
                                 "wd:snippet", "tei:trait", "wd:certitude"])

        # get the total number of rows
        trows = sum(1 for row in in_reader)
        f_in.seek(0)
        next(f_in)  # skip column headers

        for row in tqdm(in_reader, desc="retrieving IDs from the wikidata API", total=trows):
            # safeguard in case the script crashes: see which
            # entries have aldready been queried to avoid querying them again
            # queried is rebuilt at each iteration to update it with new entries
            out_reader = csv.reader(f_out, delimiter="\t")
            content = list(out_reader)  # the lines aldready present in the output csv file
            for c in content:
                queried.append([c[0], c[2]])  # add entry's xml:id and tei:name to a list
            f_out.seek(0, 0)

            in_data = [row[2], row[3]]  # input data on which to launch a query
            entry = [row[0], row[2]]  # to compare with queried

            # manage the updates: run a query only if this row hasn't been queried aldready
            if entry not in queried:
                try:
                    qdict, prev = prep_query(in_data, prev)
                    out = launch_query(qdict, config)
                    out_writer.writerow([row[0], out[0], row[2], out[1], out[2], row[3], out[3]])
                except:
                    print(f"########### ERROR ON {row[0]} ###########")
                    print(row)
                    print(qdict)
                    error = traceback.format_exc()
                    print(error)
                    sys.exit(1)

        f_in.close()

    # delete queried.json (which stores aldready ran queries)
    os.remove("tables/queried.json")
    return None


def striptag(instr):
    """
    strip html tags returned by the wikidata api
    :param instr: input string to strip tags from
    :return: the string with stripped tags
    """
    outstr = re.sub(r"<.*?>", "", instr)
    return outstr

    """with open("tables/wd_nametable.tsv", mode="r", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        prev = {}  # dictionary to store the previous loop entry
        for row in reader:
            in_data = [row[2], row[3]]  # input data on which to launch a query
            qdict, prev = prep_query(in_data, prev)
            launch_query(qdict, {"test": False, "fetch": True})"""


# ================= LAUNCH THE SCRIPT ================= #
if __name__ == "__main__":
    itemtoid()
