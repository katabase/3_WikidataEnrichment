import requests
import json
import csv
import re

from tables.matching import status, provinces, dpts, colonies, \
    countries, events, other, functions
from rgx import namebuild, rgx_complnm

# get a wikidata id from a full text query

# r_url = "https://www.wikidata.org/w/api.php?action=query&list=search&srsearch=du+resnel&format=json"
# r_headers = {
#     "User-Agent": "katabot/1.0 (https://katabase.huma-num.fr/) python/requests/2.27.1",
#     "Accept-Encoding": "gzip"
# }
# r = requests.get(r_url, headers=r_headers)
# print(r.text)

# https://www.mediawiki.org/wiki/API:Etiquette


# ================= BUILD A QUERY ================= #
def prep_query(in_data, prev):
    """
    prepare the query string: normalize first names, extract data from the tei:trait,
    order the querystring
    :param in_data: input data: a list of the 3rd and 4th entries of the csv
    :param prev: the dictionary built in the previous loop (in case a tei:name has "le même" as value)
    :return:
    """
    fname = ""  # first name of a person or additional info if the tei:name is not about a person
    lname = ""  # last name of a person or main info if the tei:name is not about a person
    nobname_sts = ""  # name owned by a person if they are nobility
    sts_title = ""  # nobility titles of a person
    dates = ""  # dates of life/death of a person (in tei:trait or in tei:name for historical events)
    function = ""  # functions occupied by a person (in tei:trait)
    rebuilt = False  # wether a person's first name has been rebuilt from an abbreviation
    # abv = None  # wether a person's first name contains abbreviations
    name = in_data[0]  # tei:name
    trait = in_data[1]  # tei:trait

    # extract content in parenthesis
    parenth = re.search(r"\(.+\)?", name)  # check if there's text inside the parenthesis
    if parenth is not None:
        inp = re.sub(r"\(|\)", "", parenth[0])  # text in parenthesis
        # firstnm, matchstr, rebuilt, abv = namebuild(inp)  # try to extract the full name
        firstnm, matchstr, rebuilt = namebuild(inp)  # try to extract the full name
    else:
        inp = ""
        matchstr = ""  # so that the case 4 condition doesn't fail

    # =========== PARSE THE NAME =========== #
    # CASE 1 - the name is the same as the one in the previous item: build this item's data w/ data from the prev item
    if re.match("(le|la)\smême", name.lower()):
        qdata = prev
        prev = qdata
        return qdata, prev  # at this point, there's no tei:trait with meaningful
        #                     info when tei:name == "le même" => stop the function there

    # CASE 2 - "DIVERS / DOCUMENTS"
    elif re.match(r"([Dd]((OCUMENT|ocument)[Ss]?|(IVERS|ivers))|\s)+", name):
        lname = ""

    # CASE 3 - CHARTS
    elif re.search("[Cc](HARTE|harte)[sS]?", name) is not None:
        lname = "charter"

    # CASE 4 - it contains geographic data:
    elif any(p in re.sub(r"(\.|,|(\s-)|(-\s))+", " ", name).lower().split() for p in provinces) \
            or any(d in re.sub(r"(\.|,|(\s-)|(-\s))+", " ", name).lower().split() for d in dpts) \
            or any(c in re.sub(r"(\.|,|(\s-)|(-\s))+", " ", name).lower().split() for c in colonies) \
            or any(c in re.sub(r"(\.|,|(\s-)|(-\s))+", " ", name).lower().split() for c in countries.keys()):
        if matchstr == "" and not any(s in name.lower() for s in status):  # check that it's not a name
            # clean the string
            name = re.sub(r"(^\.?\s+|.?\s+.?$)", "", name).lower()
            # remove extra noise : persons
            if name == "pelet de la lozère" or name == "anne de bretagne" or name == "jeanne de bourgogne":
                fname = re.search(r"^[a-z]+", name)[0]
                lname = re.search(r"de", name)[0]
            # extract info about the churches
            elif re.search(r"[ée]glises?", name):
                for d in dpts:
                    if d in name:
                        lname = d
                        fname = "religious buildings"
            # extract other specific places
            elif any(o in name for o in other.keys()):
                for o in other:
                    if o in name:
                        fname = other[o]
                        lname = re.search(r"^[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ]+[a-zàáâäéèêëíìîïòóôöúùûüøœæç]*", name)  # name of city
            # remove the events: assign the event to fname, geographical data to lname, dates to lname
            elif any(e in name for e in events.keys()):
                for k, v in events.items():
                    if k in name:
                        fname = v
                        if re.search(r"\d{4}", name) is not None:
                            dates += re.search(r"\d{4}", name)[0] + " "
                        for c in countries.keys():
                            if c in name:
                                lname = countries[c]
                        for p in provinces:
                            if p in name:
                                lname = p
                        for c in colonies:
                            if c in name:
                                lname = c
                        for d in dpts:
                            if d in name:
                                lname = d
            # all the other cases, where we have just the name or other, marginal and uselsee
            # info (aka, info that we cannot easily pass to wikidata). the info added to fname
            # is not necessarily meaningful: it is what works best to get the first result to
            # be the good one on wikidata
            else:
                for c in countries.keys():
                    if c in name:
                        lname = countries[c]
                for p in provinces:
                    if p in name:
                        fname = "province"
                        lname = p
                for c in colonies:
                    if c in name:
                        fname = "french"
                        lname = c
                for d in dpts:
                    if d in name:
                        fname = "french department"
                        lname = d

    # CASE 5 - it's related to an historical event

    # SMH "REVOLUTION FRANCAISE" IS NOT MATCHED

    elif any(e in re.sub(r"(\.|,|(\s-)|(-\s))+", " ", name).lower().split() for e in events):
        # specicy that the "revolution" is the french revolution if there's no other info except dates
        name = re.sub(r"(\.|,|(\s-)|(-\s))+", " ", name).lower()
        print(name)
        if re.search(r"^(r[eé]volution|\s|de|\d{4})*$", name):
            print("ok")
            fname = "french revolution"
            if re.search(r"\d{4}", name) is not None:
                dates = re.search(r"\d{4}", name)[0]
            else:
                dates = ""
        elif re.search(r"^(guerre|\s|de|\d{4})*$", name):
            print("ok")
            fname = "french war"
            if re.search(r"\d{4}", name) is not None:
                dates = re.search(r"\d{4}", name)[0]
            else:
                dates = ""
        else:
            for k, v in events.items():
                if k in name or name == k:
                    fname = v
                    if re.search(r"\d{4}", name) is not None:
                        dates += re.search(r"\d{4}", name)[0] + " "

        print(fname, lname, dates)
        print("_________________")


    # CASE 5 - it's (considered to be) a name (yay!)
    else:
        if inp != "":
            # check whether the name is a nobility (they have a special structure and must be
            # treated differently : "Title name (Actual name, title)").
            sts = False  # check wether a status name has been found
            for k, v in status.items():
                if k in inp.lower():
                    # extract the person's surname by suppressing the noise (aka, non-surname data)
                    # a surname is something that: 
                    # - isn't a title (isn't in status)
                    # - isn't a name (doesn't match matchstring)
                    # - does not contain only lowercase letters
                    # this horrendous series of regexes pretty much removes all the noise
                    inp = inp.replace(matchstr, "")  # delete strings matched by namebuild (deletes status titles if they were matched)
                    inp = re.sub(f",?\s?(le|la|l')?\s?{k}(\s(de|de\sla|du|d'|,)*(\s|$))*", "", inp)
                    inp = re.sub(r"(^|\s)+(puis|dit)", "", inp)
                    inp = re.sub(r"(^|\s)+([Ll]e|[Ll]a|[Dd]e(s)?|[Dd]u)+(\s|$)", "", inp)
                    inp = re.sub(r"(^|\s)+(et|\.)(\s|$)", " ", inp)
                    inp = re.sub(r"(l'|,)", "", inp)
                    if re.match(r"([A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]+)([A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ])", inp):
                        mo = re.match(r"([A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]+)([A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ])", inp)
                        inp = re.sub(r"([A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]*)([A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ])",
                                     f"{mo[1]} {mo[2]}", inp)  # add a space between two accented words
                    inp = re.sub(r"(\s|^)[a-zàáâäéèêëíìîïòóôöúùûüøœæç]+(\.|,|\s|$)", " ", inp)  # del lowercase words
                    inp = re.sub(r"\s+", " ", inp)
                    
                    sts = True  # a status has been found ; the string will be worked differently
                    if v != "":
                        sts_title += f"{v} "  # add the title to the dictionary
            
            if sts is True:
                fname = firstnm
                lname = inp.lower()
                nobname_sts = name.replace(parenth[0], "")

            # if no status info has been found but there is data in parenthesis, the
            # full name is much simpler to extract: the format is "Last name (first name)"
            # first extract the first name from the parenthesis and try to build a full name;
            # then, extract the last name outside the parenthesis
            else:
                # if the string is not completely empty, try to extract a name that might have gone unnoticed
                if not re.search("^\s*(\s|d'|de|dit|,)*\s*$", inp.replace(matchstr, "")):
                    if len(namebuild(inp.replace(matchstr, ""))[0]) > 0:  # if we can match a name
                        # rebuild additional names that names that have gone unnoticed:
                        # extract a name, assign it to addnm and build fname
                        addnm = namebuild(inp.replace(matchstr, ""))[0]  # the rebuild name; the abv and rebuilt
                        #                                                  returned by namebuild() have aldready been
                        #                                                  assigned when calling namebuld the 1st time
                        # print(f"input string: {inp}")
                        # print(f"first name : {firstnm}")
                        # print(f"additional name: {addnm}")
                        if "père" in inp and "Dumas" in name:
                            add = "père"
                        elif "fils" in inp and "Dumas" in name:
                            add = "fils"
                        else:
                            add = ""

                        # reorder the name : relative position of matchstr and addnm in inp
                        if inp.find(matchstr) < inp.find(addnm):
                            fname = re.sub("\s+", " ", f"{firstnm} {add} {addnm}").lower()
                        else:
                            fname = re.sub("\s+", " ", f"{addnm} {add} {firstnm}").lower()
                else:
                    # match alexandre dumas père / fils (quite a lot of entries on them)
                    if re.search(r"(^|\s+)(père|fils)(\s+|$)", name.replace(parenth[0], "")) \
                            and re.search(r"(^|\s+)D(UMAS|umas)(\s+|$)", name):
                        add = re.search(r"(^|\s+)(père|fils)(\s+|$)", name.replace(parenth[0], ""))[0]
                    else:
                        add = ""
                    fname = re.sub("\s+", " ", f"{firstnm} {add}").lower()

                # clean the last name
                lname = name.replace(parenth[0], "").lower()
                lname = re.sub("(^|\s+)(père|fils)(\s+|$)", " ", lname)
                lname = re.sub(r",|\.", "", lname)
                lname = re.sub("\s+", " ", lname)

        # if the tei: name doesn't contain parenthesis, it is generally pretty clean
        # and structured but its structure is too loose to do any regex matching =>
        # clean it a bit and save it as lname
        else:
            lname = re.sub(r"\.|,|(^\s)|(\s$)|(-\s?$)|(^\s?-)|\(|\)|\"", "", name).lower()

    # =========== PARSE THE trait =========== #
    # try to extract the birth/death dates
    if re.search(r"\d{4}", trait):
        # get some context on the date to determine whether it's a birth/death date or another date
        if re.search(r"(^|\s|,|\.)[Nn](.|\s|ée?).+?(?=\d{4})\d{4}", trait):
            context = re.search(r"(^|\s|,|\.)[Nn](.|\s|ée?).+?(?=\d{4})\d{4}", trait)[0]
            dates += re.search(r"\d{4}", context)[0] + " "
        if re.search(r"(^|\s|,|\.)((M\.|m\.)|[Mm](\s|orte?)).+?(?=\d{4})\d{4}", trait):
            context = re.search(r"(^|\s|,|\.)((M\.|m\.)|[Mm](\s|orte?)).+?(?=\d{4})\d{4}", trait)[0]
            dates += re.search(r"\d{4}", context)[0] + " "
        elif re.search(r"(^|\s|,|\.)([Dd]écap|[Aa]ssa|[Tt]uée?|[Ff]usi|[Gg]uil).+?(?=\d{4})\d{4}", trait):
            context = re.search(r"(^|\s|,|\.)([Gg]uil|[Dd]écap|[Aa]ssa|[Tt]uée?|[Ff]usi).+?(?=\d{4})\d{4}", trait)[0]
            dates += re.search(r"\d{4}", context)[0] + " "

    # try to extract a function
    for w in trait.split():
        for k, v in functions.items():
            if w == k:
                function += f"{v} "

    # clean the functions to keep only one function : several functions are often extracted,
    # we keep only one (two functions => less wikidata results)
    if len(set(function.split())) == 1:
        function = function.split()[0]  # removing duplicates

    # select the most important functions if there are several functions
    elif len(set(function.split())) > 1:
        # sometimes, the relation of a person to others is described ; others'
        # role can be given in the trait too and matched in "function" ; in this
        # case, try to match the titles in a relation
        if any(re.search(f"(du|de|d'|par)\s(le|la|l')?\s?{k}", trait) for k in functions.keys()):
            for f in function.split():
                for k, v in functions.items():
                    # match the original term (not its wikidata equivalent) and delete it
                    if f == v:
                        if re.search(f"(du|de|d')\s(le|la|l')?\s?{k}", trait):
                            function = re.sub(f"{f}", "", function)
        # "writer" is often noise, not a person's main function
        elif "writer" in function.split():
            function = re.sub(r"(writer|\s$)", "", function)
        # select the most important function amongst other functions
        elif "general" in function.split() and "marshal" in function.split() \
                or "military" in function.split() and "marshal" in function.split():
            function = "marshal"  # marshal is more common than general in wikidata
        elif "general" in function.split() and "military" in function.split():
            function = "general"
        elif "emperor" in function.split():
            function = "emperor"
        # if nothing else works, only keep the most important terms
        else:
            function = function.split()[0]

    qdata = {
        "fname": fname,
        "lname": lname,
        "nobname_sts": nobname_sts,
        "status": sts_title,
        "dates": dates,
        "function": function,
        "rebuilt": rebuilt,
        # "abv": abv
    }
    qdata_prev = qdata  # in case an entry is labeled "le même", aka the same person as the entry before

    # return
    return qdata, qdata_prev


def request(qstr):
    """
    run a full text search on wikidata and return the result
    :param qstr: the query string
    :return:
    """
    # remove duplicate words from qstr
    qstr = qstr.lower().split()
    qstr_ddp = " ".join(sorted(set(qstr), key=qstr.index))

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
        "srsearch": qstr_ddp.strip(),
        "srlimit": 1,
        "format": "json"
    }

    print(qstr_ddp)

    # launch query and return the person's wikidata id
    r = requests.get(url, params=params, headers=headers)
    js = r.json()
    try:
        w_id = js["query"]["search"][0]["title"]
    except IndexError:
        w_id = ""
    except KeyError:
        w_id = ""
    return w_id


def relaunch_query(qstr, qdict, avail):
    """
    relaunch a query for launch_query() if it fails : remove one parameter at
    a time and relaunch the query
    :return:
    """
    if len(list(qdict["dates"].split())) == 2:
        dates = list(qdict["dates"].split())
        qstr_dates = re.sub(r"\s+", " ", qstr.replace(dates[0], ""))
        w_id = request(qstr_dates)
        if w_id == "":
            qstr_dates = re.sub(r"\s+", " ", qstr.replace(dates[1], ""))
            w_id = request(qstr_dates)
    for a in avail:
        qstr = re.sub(r"\s+", " ", qstr.replace(qdict[a], ""))
        w_id = request(qstr=qstr)
        if w_id != "":
            break  # info has been found. stop looping
        else:
            qstr = re.sub(r"\s+", " ", f"{qstr} {qdict[a]}")  # add the removed parameter
            #                                                   before looping on another param
    return w_id


def launch_query(qdict):
    """
    build a wikidata full text search from a dictionary of structured data
    (see build_query()).
    :param qdict: the query string (i.e., name for which we want a wikidata id)
    :return:
    """
    # check which info is available and build the query string.
    avail = []  # dictionary indicating which infos are available in qdict
    for k, v in qdict.items():
        if isinstance(v, str):
            # if there is data for a key in qdict (there's a date...) ; flname and lname
            # are not to be excluded, except if rebuilt is true (see below)
            if not re.match(r"^\s*$", v) and not re.search("(fname|lname)", k):
                avail.append(k)

    # build query string. the first query is made with all available data
    qstr = re.sub(r"\s+", " ", f"{qdict['fname']} {qdict['lname']} {qdict['status']} \
            {qdict['nobname_sts']} {qdict['dates']} {qdict['function']}").lower()

    # CHECK THE RESULTS OF DIFFERENT ORDER OF ELEMENTS IN THE ABOVE STRING
    # ########################################################

    # launch query if there is info on which to be queried
    if not re.match(r"^\s*$", qstr):
        w_id = request(qstr=qstr)

        # extra behaviour if there's no result : delete first name if it was rebuilt,
        # remove one of qdict parameters per query (except for lname and fname)
        if w_id == "":
            # if there's a nobility name, try to remove the fname or lname or both (conditions 1, 2, 3)
            # if all else fails, relaunch_query without fname or lname
            if qdict["nobname_sts"] != "":
                if qdict["fname"] != "":
                    qstr = qstr.replace(qdict["fname"], "")
                    w_id = request(qstr=qstr)
                if w_id == "" and qdict["lname"] != "":
                    qstr = qstr.replace(qdict["lname"], "")
                    if not re.match(r"^\s*?", qdict["fname"]):
                        qstr = qstr + f" {qdict['fname']} "
                    w_id = request(qstr=qstr)
                if w_id == "" and qdict["fname"] != "" and qdict["lname"] != "":
                    qstr = qstr.replace(qdict["fname"], "")
                    qstr = qstr.replace(qdict["lname"], "")
                    w_id = request(qstr=qstr)
                if w_id != "":
                    relaunch_query(qstr=qstr, qdict=qdict, avail=avail)

            # remove a parameter and relaunch the query
            elif len(avail) >= 1:
                w_id = relaunch_query(qstr=qstr, qdict=qdict, avail=avail)

            # relaunch the query without the rebuilt name
            elif qdict["rebuilt"] is True:
                qstr = qstr.replace(qdict["fname"], "")
                w_id = request(qstr=qstr)
                if w_id == "" and len(avail) >= 1:
                    w_id = relaunch_query(qstr=qstr, qdict=qdict, avail=avail)

            # if all else fails, remove the first name (a french version of the name
            # can be in the catalogs, while a foreign version only is on wikidata) and relaunch the query
            if w_id == "" and not re.search(r"^\s*$", qdict["fname"]):
                qstr = re.sub(r"\s+", " ", qstr.replace(qdict["fname"], ""))
                w_id = request(qstr=qstr)
                if w_id == "" and len(avail) >= 1:
                    w_id = relaunch_query(qstr=qstr, qdict=qdict, avail=avail)

    # if after all no id is found
    else:
        w_id = ""

    return w_id


def itemtoid():
    """
    launch the query on all entries of nametable_in.tsv
    :return:
    """
    with open("tables/wd_nametable.tsv", mode="r", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter="\t")
        prev = {}  # dictionary to store the previous loop entry
        for row in reader:
            in_data = [row[2], row[3]]  # input data on which to launch a query
            qdict, prev = prep_query(in_data, prev)
            # launch_query(qdict)


# ================= LAUNCH THE SCRIPT ================= #
if __name__ == "__main__":
    # launch_query("merzbow")
    itemtoid()
