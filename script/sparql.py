from SPARQLWrapper import SPARQLWrapper, JSON
from pathlib import Path
from tqdm import tqdm
import json
import os

from .utils.idset import build_idset
from .utils.paths import OUT, TABLES
from .utils.classes import *


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

    # the query is too long so we divide it in 3. the first 2 are on persons,
    # the last on works of art
    query1 = """
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?instance
               ?instanceL ?gender ?genderL ?citizenship ?citizenshipL ?lang ?langL 
               ?deathmanner ?deathmannerL ?birthplace ?birthplaceL ?deathplace ?deathplaceL
               ?residplace ?residplaceL ?burialplace ?burialplaceL
          
        WHERE {
          BIND (wd:TOKEN AS ?id)
          
          OPTIONAL {
            ?instance ^wdt:P31 ?id .
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
          OPTIONAL {
            ?id wdt:P103 ?lang .
            ?lang rdfs:label ?langL .
            FILTER (langMatches(lang(?langL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P1196 ?deathmanner .
            ?deathmanner rdfs:label ?deathmannerL .
            FILTER (langMatches(lang(?deathmannerL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P19 ?birthplace .
            ?birthplace rdfs:label ?birthplaceL .
            FILTER (langMatches(lang(?birthplaceL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P570 ?deathplace .
            ?deathplace rdfs:label ?deathplaceL .
            FILTER (langMatches(lang(?deathplaceL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P551 ?residplace .
            ?residplace rdfs:label ?residplaceL .
            FILTER (langMatches(lang(?residplaceL), "EN"))
          }
          OPTIONAL {
            ?id wdt:119 ?burialplace .
            ?burialplace rdfs:label ?burialplaceL .
            FILTER (langMatches(lang(?burialplaceL), "EN"))
          }
        } # LIMIT 1 # => only first of each item
    """.replace("TOKEN", w_id)

    query2 = """
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?educ ?educL ?religion ?religionL ?occupation ?occupationL 
               ?award ?awardL ?position ?positionL 
               ?member ?memberL ?nobility ?nobilityL
               ?workcount ?conflictcount
               ?image ?signature
               ?birth ?death 

        WHERE {
          BIND (wd:TOKEN AS ?id)
          
          OPTIONAL {
            ?id wdt:P69 ?educ .
            ?educ rdfs:label ?educL .
            FILTER (langMatches(lang(?educL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P140 ?religion .
            ?religion rdfs:label ?religionL .
            FILTER (langMatches(lang(?religionL), "EN"))
          }
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
          OPTIONAL {?id wdt:P569 ?birth .}
          OPTIONAL {?id wdt:P570 ?death .}
          OPTIONAL {?id wdt:P18 ?img .}
          OPTIONAL {?id wdt:P109 ?signature .}
          
          OPTIONAL {
            SELECT ?id (COUNT(?work) AS ?workcount)  # number of notable works
            WHERE {?work wdt:P50 ?id.} GROUP BY ?id
          }
          OPTIONAL {
            SELECT ?id (COUNT(?conflict) AS ?conflictcount)  # number of conflicts participated in
            WHERE {?id wdt:P607 ?conflict.} GROUP BY ?id
          }
        } # LIMIT 1
    """

    query3 = """
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?titleL ?inception ?author ?authorL ?pub ?pubL ?pubplace ?pubplaceL ?pubdate
               ?creator ?creatorL ?material ?materialL ?height ?genre ?genreL ?creaplace ?creaplaceL 

        WHERE {
          BIND (wd:TOKEN AS ?id)
          
          OPTIONAL {?id wdt:P1476 ?titleL .}
          OPTIONAL {?id wdt:P571 ?inception .}
          OPTIONAL {
            ?id wdt:P50 ?author .
            ?author rdfs:label ?authorL .
            FILTER (langMatches(lang(?authorL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P123 ?pub .
            ?pub rdfs:label ?pubL .
            FILTER (langMatches(lang(?pubL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P291 ?pubplace .
            ?pubplace rdfs:label ?pubplaceL .
            FILTER (langMatches(lang(?pubplaceL), "EN"))
          }
          OPTIONAL {?id wdt:P577 ?pubdate .}
          OPTIONAL {
            ?id wdt:P170 ?creator .
            ?creator rdfs:label ?creatorL .
            FILTER (langMatches(lang(?creatorL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P186 ?material .
            ?material rdfs:label ?materialL .
            FILTER (langMatches(lang(?materialL), "EN"))
          }
          OPTIONAL {?id wdt:P2048 ?height .}
          OPTIONAL {
            ?id wdt:P136 ?genre .
            ?genre rdfs:label ?genreL .
            FILTER (langMatches(lang(?genreL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P1071 ?creaplace .
            ?creaplace rdfs:label ?creaplaceL .
            FILTER (langMatches(lang(?creaplaceL), "EN"))
          }
        } # LIMIT 1
    """

    endpoint = SPARQLWrapper(
        "https://query.wikidata.org/sparql",
        agent="katabot/1.0 (https://katabase.huma-num.fr/) python/SPARQLwrapper/2.0.0"
    )

    try:
        # launch the queries
        endpoint.setQuery(query1)
        endpoint.setReturnFormat(JSON)
        results1 = endpoint.queryAndConvert()
        out1 = result_tojson(results1)
    except:
        Error.sparql_error_handle(query1, w_id)

    try:
        endpoint.setQuery(query2)
        endpoint.setReturnFormat(JSON)
        results2 = endpoint.queryAndConvert()
        out2 = result_tojson(results2)
    except:
        Error.sparql_error_handle(query2, w_id)

    try:
        endpoint.setQuery(query3)
        endpoint.setReturnFormat(JSON)
        results3 = endpoint.queryAndConvert()
        out3 = result_tojson(results3)
    except:
        Error.sparql_error_handle(query3, w_id)

    # parse the result into a json
    for o in [out1, out2, out3]:
        print(o)
        for k, v in o.items():
            out[k] = v

    return out


def result_tojson(wd_result):
    """
    transform the JSON returned by wikidata into a more elegant JSON
    :param wd_result: the json returned by wikidata
    :return: a cleaner JSON
    """
    out_dict = {}  # a cleaned json to built out
    var = wd_result["head"]["vars"]  # the queried variables (used as keys to out_dict)

    for bind in wd_result["results"]["bindings"]:
        # build out_dict
        for v in var:
            if v not in out_dict:
                if v in bind:
                    out_dict[v] = [Strings.clean(bind[v]["value"])]
                else:
                    out_dict[v] = []
            else:
                if v in bind and Strings.clean(bind[v]["value"]) not in out_dict[v]:
                    # avoid duplicates: compare all strings in out[v] to see if they match with out[v]
                    same = False
                    for o in out_dict[v]:
                        if Strings.compare(
                                Strings.clean(bind[v]["value"]), o
                        ) is True:  # if there's a matching comparison
                            same = True

                    if same is False:
                        out_dict[v].append(Strings.clean(bind[v]["value"]))

    return out_dict


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

    # create the output file and log file
    fp_json = f"{OUT}/wikidata/sparql_out.json"
    fp_log = f"{LOGS}/log_sparql.txt"
    if not os.path.isfile(fp_json):
        Path(fp_json).touch()
    if not os.path.isfile(fp_log):
        Path(fp_log).touch()

    # launch the query on all ids; if the output file is not empty,
    # read the contents and update it;
    # else, just write the contents
    for w_id in tqdm(idlist):
        log = open(fp_log, mode="r", encoding="utf-8")
        done = log.read().split()  # list of queried wikidata ids
        if w_id not in done:
            done = []  # empty the long list of queried ids
            log.close()  # close the file to save memory
            with open(fp_json, mode="r+") as fh:
                if os.stat(fp_json).st_size > 0:
                    queried = json.load(fh)
                    queried[w_id] = sparql(w_id)
                    fh.seek(0)
                    json.dump(queried, fh, indent=4)
                else:
                    queried = {w_id: sparql(w_id)}
                    json.dump(queried, fh, indent=4)
                Log.log_done(mode="sparql", data=w_id)

    return None


if __name__ == "__main__":
    launch()
