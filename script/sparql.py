from SPARQLWrapper import SPARQLWrapper, JSON
from pathlib import Path
from tqdm import tqdm
import traceback
import json
import sys
import re
import os

from .utils.idset import build_idset
from .utils.paths import OUT, TABLES


# ---------------------------------------------------
# launch sparql queries and save the result to a json
# ---------------------------------------------------


def sparql(w_id):
    """
    launch a sparql query and return the result

    wikidata json output:
    {
        'head': {'vars': ['list', 'of', 'variable', 'queried']},  # queried variables wether or not there's a result
        'results':
            {'bindings':
                [
                    {
                        # first series of results; only variables with a result are in bindings (as keys)
                        'var1': {'type': 'datatype', 'value': 'actual result 1'},
                        'var2': {'type': 'datatype', 'value': 'actual result 1'}
                    }, {
                        # second series of results
                        'var1': {'type': 'datatype', 'value': 'actual result 2'},
                        'var2': {'type': 'datatype', 'value': 'actual result 2'}
                    }
                ]
            }
    }

    out structure: a dictionary mapping to query keys (var) a list of results:
    - either an empty list (no result for that key)
    - or a list of different query results (wikidata returns a cartesian product: a
      combination of all possible combination of query results. if wikidata returns:
      if a wikidata query is launched on 2 values and each return 3 different results,
      the results will be a list of 3x3=9 possible combinations, which mean there will
      be duplicates. in turn, we need to deuplicate)

    example:
    out = {
        'instance': ['Q5'],
        'instanceL': ['human'],
        'gender': ['Q6581097'],
        'genderL': ['male'],
        'citizenship': ['Q142'],
        'citizenshipL': ['France'],
        'birth': ['1788-01-06T00:00:00Z'],
        'death': ['1868-05-06T00:00:00Z'],
        'occupation': ['Q40348', 'Q82955', 'Q2135469', 'Q1930187'],
        'occupationL': ['lawyer', 'Lawyer', 'politician', 'legal counsel', 'journalist'],
        'award': ['Q10855212', 'Q372160'],
        'awardL': ['Commander of the Legion of Honour', 'Montyon Prizes'],
        'position': ['Q3044918', 'Q54996617', 'Q63442227'],
        'positionL': [
            'member of the French National Assembly',
            'President of the State Council of France',
            "Conseiller général de l'Yonne"
        ],
        'member': ['Q337543'],
        'memberL': ['Académie des Sciences Morales et Politiques'],
        'nobility': ['Q185902'],
        'nobilityL': ['viscount'],
        'image': []
    }


    :param w_id:
    :return:
    """
    out = {}  # dictionnary to store the output
    query = """
                PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?instance ?instanceL ?gender ?genderL ?citizenship ?citizenshipL
          ?birth ?death ?occupation ?occupationL ?award ?awardL ?position ?positionL 
          ?member ?memberL ?nobility ?nobilityL ?image
        
        WHERE {
          BIND (wd:TOKEN AS ?id)
          
          OPTIONAL {
            ?id wdt:P31 ?instance .
            ?instance rdfs:label ?instanceL .
            FILTER (langMatches(lang(?instanceL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P21 ?gender .
            ?gender rdfs:label ?genderL .
            FILTER (langMatches(lang(?genderL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P27 ?citizenship .
            ?citizenship rdfs:label ?citizenshipL .
            FILTER (langMatches(lang(?citizenshipL), "EN"))
          }
          OPTIONAL {?id wdt:P569 ?birth .}
          OPTIONAL {?id wdt:P570 ?death .}
          OPTIONAL {
            ?id wdt:P106 ?occupation .
            ?occupation rdfs:label ?occupationL .
            FILTER (langMatches(lang(?occupationL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P166 ?award .
            ?award rdfs:label ?awardL .
            FILTER (langMatches(lang(?awardL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P39 ?position .
            ?position rdfs:label ?positionL .
            FILTER (langMatches(lang(?positionL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P463 ?member .
            ?member rdfs:label ?memberL .
            FILTER (langMatches(lang(?memberL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P97 ?nobility .
            ?nobility rdfs:label ?nobilityL .
            FILTER (langMatches(lang(?nobilityL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P18 ?img .
          }
        } # LIMIT 1 => only first of each item
    """.replace("TOKEN", w_id)

    endpoint = SPARQLWrapper(
        "https://query.wikidata.org/sparql",
        agent="katabot/1.0 (https://katabase.huma-num.fr/) python/SPARQLwrapper/2.0.0"
    )

    try:
        # launch the query
        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        results = endpoint.queryAndConvert()

        # parse the result into a json
        var = results["head"]["vars"]  # variables to get the result

        for bind in results["results"]["bindings"]:
            # done = {}  # dictionary to contain the processed results
            # build out
            for v in var:
                if v not in out:
                    if v in bind:
                        out[v] = [clean(bind[v]["value"])]
                    else:
                        out[v] = []
                else:
                    if v in bind and clean(bind[v]["value"]) not in out[v]:
                        # avoid duplicates: compare all strings in out[v] to see if they match with out[v]
                        same = False
                        for o in out[v]:
                            if compare(clean(bind[v]["value"]), o) is True:  # if there's a matching comparison
                                # print("YYYYYYYYYYYYYYYYYYYYYYYY")
                                same = True

                        if same is False:
                            out[v].append(clean(bind[v]["value"]))
                    else:
                        pass

                    # if v not in bind and

        print(out)

    except:
        print(f"########### ERROR ON {w_id} ###########")
        error = traceback.format_exc()
        print("query text on which the error happened:")
        print(query)
        print(error)
        sys.exit(1)

    return out


def launch():
    """
    launch queries on all wikidata ids stored in tables/wikidata_id.txt
    and save the result to out/wikidata/sparql_out.json
    :return: None
    """
    # build a unique list of ids and save it to a file
    build_idset()

    # parse the wikidata ids
    with open(f"{TABLES}/id_wikidata.txt", mode="r") as f:
        idlist = f.read().split()

    # create the output file
    fpath = f"{OUT}/wikidata/sparql_out.json"
    if not os.path.isfile(fpath):
        Path(fpath).touch()

    # launch the query on all ids; if the output file is not empty,
    # read the contents and update it;
    # else, just write the contents
    for w_id in tqdm(idlist):
        with open(fpath, mode="r+") as fh:
            if os.stat(fpath).st_size > 0:
                queried = json.load(fh)
                queried[w_id] = sparql(w_id)
                fh.seek(0)
                json.dump(queried, fh, indent=4)
            else:
                queried = {w_id: sparql(w_id)}
                json.dump(queried, fh, indent=4)

    return None


def clean(s):
    """
    clean the input string s:
    - strip the beginning of a wikidata url to only keep the wikidata id
    - clean the date by removing the time
    to keep only the id : http://www.wikidata.org/entity/
    :param s: the input string: a wikidata url
    :return: url, the id alone.
    """
    s = s.replace("http://www.wikidata.org/entity/", "")
    s = re.sub(r"T\d{2}:\d{2}:\d{2}Z$", "", s)
    return s


def compare(input, compa):
    """
    compare two strings to check if they're the same without punctuation and
    capitals
    :param input: input string
    :param compa: string to compare input with
    :return:
    """
    punct = ['!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '-',
             '+', '=', '{', '}', '[', ']', ':', ';', '"', "'", '|',
             '<', '>', ',', '.', '?', '/', '~', '`']
    input = input.lower()
    compa = compa.lower()
    for p in punct:
        input = input.replace(p, "")
        compa = compa.replace(p, "")
    input = re.sub(r"\s+", " ", input)
    compa = re.sub(r"\s+", " ", compa)
    input = re.sub(r"(^\s|\s$)", "", input)
    compa = re.sub(r"(^\s|\s$)", "", compa)

    # print(input, "|", compa)
    same = (input == compa)  # true if same, false if not
    return same

if __name__ == "__main__":
    launch()


"""
JSON RETURN FORMAT 
{'head': 
     {'vars': 
          ['instance', 'gender', 'citizenship', 'birth', 'date', 'occupation', 'award', 'position', 'member', 'nobility', 'image']
      }, 
 'results': 
     {
         'bindings': [
             {'instance': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q5'}, 
              'gender': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q6581097'}, 
              'citizenship': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q142'}, 
              'birth': {'datatype': 'http://www.w3.org/2001/XMLSchema#dateTime', 'type': 'literal', 'value': '1783-01-23T00:00:00Z'}, 
              'occupation': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q36180'}, 
              'award': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q10855271'}, 
              'position': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q21694612'}, 
              'image': {'type': 'uri', 'value': 'http://commons.wikimedia.org/wiki/Special:FilePath/Stendhal.jpg'}
              }, 
             {'instance': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q2985549'}, 
              'gender': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q6581097'}, 
              'citizenship': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q142'}, 
              'birth': {'datatype': 'http://www.w3.org/2001/XMLSchema#dateTime', 'type': 'literal', 'value': '1783-01-23T00:00:00Z'}, 
              'occupation': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q36180'}, 
              'award': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q10855271'}, 
              'position': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q21694612'}, 
              'image': {'type': 'uri', 'value': 'http://commons.wikimedia.org/wiki/Special:FilePath/Stendhal.jpg'}}
         ]
     }
 }
"""

"""
                PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?instance ?instanceL ?gender ?genderL ?citizenship 
          ?birth ?death ?occupation ?occupationL ?award ?awardL 
          ?position ?positionL ?member ?memberL ?nobility ?nobilityL ?image
        
        WHERE {
          BIND (wd:TOKEN AS ?id)
          
          OPTIONAL {
            ?id wdt:P31 ?instance .
            ?instance rdfs:label ?instanceL .
            FILTER (langMatches(lang(?instanceL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P21 ?gender .
            ?gender rdfs:label ?genderL .
            FILTER (langMatches(lang(?genderL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P27 ?citizenship .
            ?citizenship rdfs:label ?citizenshipL .
            FILTER (langMatches(lang(?citizenshipL), "EN"))
          }
          OPTIONAL {?id wdt:P569 ?birth .}
          OPTIONAL {?id wdt:P570 ?death .}
          OPTIONAL {
            ?id wdt:P106 ?occupation .
            ?occupation rdfs:label ?occupationL .
            FILTER (langMatches(lang(?occupationL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P166 ?award .
            ?award rdfs:label ?awardL .
            FILTER (langMatches(lang(?awardL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P39 ?position .
            ?position rdfs:label ?positionL .
            FILTER (langMatches(lang(?positionL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P463 ?member .
            ?member rdfs:label ?memberL .
            FILTER (langMatches(lang(?memberL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P97 ?nobility .
            ?nobility rdfs:label ?nobilityL .
            FILTER (langMatches(lang(?nobilityL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P18 ?img .
          }
        } # LIMIT 1 => only first of each item
"""