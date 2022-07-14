from SPARQLWrapper import SPARQLWrapper, SPARQLExceptions, JSON
from urllib import error
from pathlib import Path
from tqdm import tqdm
import http.client
import json
import os

from .utils.paths import OUT, TABLES, LOGS
from .utils.classes import Logs, ErrorHandlers, Converters


# ---------------------------------------------------
# launch sparql queries and save the result to a json
# ---------------------------------------------------


def launch_sparql(query, w_id):
    """
    launch a sparql query and return the result in sparql json

    handling system for the queries:
    - if there's a json parsing error or an IncompleteRead from
      the http.client module, relaunch the result in xml and convert it to
      a sparql-valid json. if this new query times out, return a json with the
      variables queried mapped to an empty list. continue with the script
    - if there's a timeout, return a json mapping the variables queried to an empty
      list. continue with the script
    - if there's any other error, it's a hard exit and the script stops
    new errors can easily be caught with new excepts

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
    (about sparql return formats: https://www.w3.org/TR/2013/REC-sparql11-overview-20130321/)

    :param query: the current query
    :param w_id: the wikidata identifier
    :return: out, either 
            - a dict with a sparql response (or a dict mapping to queried keys empty lists)
            - None, if the error is sparql_general which doesn't return a value and exits the script
    """
    out = None
    endpoint = SPARQLWrapper(
        "https://query.wikidata.org/sparql",
        agent="katabot/1.0 (https://katabase.huma-num.fr/) python/SPARQLwrapper/2.0.0"
    )
    try:
        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        results = endpoint.queryAndConvert()
        out = Converters.result_tojson(results)
    except json.decoder.JSONDecodeError:  # if there's a json parsing problem or http client error
        try:
            out = ErrorHandlers.sparql_tryxml(endpoint, query)
        except http.client.IncompleteRead:
            out = ErrorHandlers.sparql_returnempty(query)
        except SPARQLExceptions.EndPointInternalError:
            out = ErrorHandlers.sparql_returnempty(query)
    except http.client.IncompleteRead:  # if there's a json parsing problem or http client error
        try:
            out = ErrorHandlers.sparql_tryxml(endpoint, query)
        except http.client.IncompleteRead:
            out = ErrorHandlers.sparql_returnempty(query)
        except SPARQLExceptions.EndPointInternalError:
            out = ErrorHandlers.sparql_returnempty(query)
    except error.HTTPError:  # if there's a timeout
        out = ErrorHandlers.sparql_returnempty(query)
    except SPARQLExceptions.EndPointInternalError:  # if there's a timeout
        out = ErrorHandlers.sparql_returnempty(query)
    except:
        ErrorHandlers.sparql_exit(query, w_id)
    return out


def config_sparql(w_id):
    """
    prepare a sparql query, launch it and return the result in a clean way. the result is then appended
    to out/wikidata/wikidata_enrichments.json

    out structure: a dictionary mapping to query keys (var) a list of results:
    -------------
    - either an empty list (no result for that key)
    - or a list of different query results (wikidata returns a cartesian product: a
      combination of all possible combination of query results. if wikidata returns:
      if a wikidata query is launched on 2 values and each return 3 different results,
      the results will be a list of 3x3=9 possible combinations, which mean there will
      be duplicates. in turn, we need to deduplicate)

    output details:
    ---------------
              as a general rule, when a key ends with "L", it is a label
              for wikidata ID that is also queried. when a key ends with
              "ID", it is a unique identifier for the queried resource.
              when it ends with "count", the result is counting a number of
              occurrences
    out = {
        'instance': [], 'instanceL': [],     # what "category" an id belongs to (person, litterary work...)
        'gender': [], 'genderL': [],     # the gender of a person
        'citizenship': [], 'citizenshipL': [],     # citizenship
        'lang': [], 'langL': [],     # languages spoken
        'deathmanner': [], 'deathmannerL': [],     # the way a person died
        'birthplace': [], 'birthplaceL': [],     # the place a person is born
        'deathplace': [], 'deathplaceL': [],     # the place a person died
        'residplace': [], 'residplaceL': [],     # the place a person lived
        'burialplace': [], 'burialplaceL': [],     # where a person is buried
        'educ': [], 'educL': [],     # where a person studied
        'religion': [], 'religionL': [],     # a person's religion
        'occupation': [], 'occupationL': [],     # general description of a person's occupation
        'award': [], 'awardL': [],     # awards gained
        'position': [], 'positionL': [],     # precise positions held by a person
        'member': [], 'memberL': [],     # institution a person is member of
        'nobility': [], 'nobilityL': [],   # nobility titles
        'workcount': [],     # number of works (books...) documented on wikidata
        'conflictcount': [],     # number of conflicts (wars...) a person has participated in
        'image': [],     # url to the portrait of a person
        'signature': [],    # url to the signature of a person
        'birth': [], 'death': [],     # birth and death dates

        'title': [],     # title of a work of art / book...
        'inception': [],     # date a work was created or published
        'author': [], 'authorL': [],     # author of a book
        'pub': [], 'pubL': [],     # publisher of a work
        'pubplace': [], 'pubplaceL': [],     # place a work was published
        'pubdate': [],     # date a work was published
        'creator': [], 'creatorL': [],     # creator of a work of art
        'material': [], 'materialL': [],     # material in which a work of art is made
        'height': [],     # height of a work of art
        'genre': [], 'genreL': [],     # genre of a work or genre of works created by a person
        'movement': [], 'movementL': [],     # movement in which a person or an artwork are inscribed
        'creaplace': [], 'creaplaceL': [],     # place where a work was created

        'viafID': [],     # viaf identifier
        'bnfID': [],     # bibliothèque nationale de france ID
        'isniID': [],      # isni id
        'congressID': [],      # library of congress identifier
        'idrefID': []      # idref identifier
    }

    :param w_id: the wikidata id queried
    :return: out, a dictionnary containing the results for that id
    """
    out = {}  # dictionnary to store the output

    # the query is too long so we divide it in 3. the first 2 are on persons,
    # the last on works of art
    query1 = """
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT DISTINCT ?instance ?instanceL 
                        ?gender ?genderL ?citizenship ?citizenshipL ?lang ?langL 
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
        }
    """.replace("TOKEN", w_id)

    query2 = """
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT DISTINCT ?educ ?educL ?religion ?religionL ?occupation ?occupationL 
                        ?award ?awardL ?position ?positionL ?member ?memberL ?nobility ?nobilityL
                        ?workcount ?conflictcount ?img ?signature ?birth ?death 

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
        }
    """.replace("TOKEN", w_id)

    query3 = """
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT DISTINCT ?title ?inception ?author ?authorL ?pub ?pubL ?pubplace ?pubplaceL ?pubdate
                        ?creator ?creatorL ?material ?materialL ?height ?genre ?genreL ?movement ?movementL
                        ?creaplace ?creaplaceL 

        WHERE {
          BIND (wd:TOKEN AS ?id)
          
          OPTIONAL {?id wdt:P1476 ?title .}
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
            ?id wdt:P135 ?movement .
            ?movement rdfs:label ?movementL .
            FILTER (langMatches(lang(?movementL), "EN"))
          }
          OPTIONAL {
            ?id wdt:P1071 ?creaplace .
            ?creaplace rdfs:label ?creaplaceL .
            FILTER (langMatches(lang(?creaplaceL), "EN"))
          }
        }
    """.replace("TOKEN", w_id)

    query4 = """
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>

        SELECT DISTINCT ?viafID ?bnfID ?isniID ?congressID ?idrefID

        WHERE {
          BIND (wd:TOKEN AS ?id)
          OPTIONAL {?id wdt:P214 ?viafID .}
          OPTIONAL {?id wdt:P268 ?bnfID .}
          OPTIONAL {?id wdt:P213 ?isniID .}
          OPTIONAL {?id wdt:P244 ?congressID .}
          OPTIONAL {?id wdt:P269 ?idrefID .}
        } LIMIT 1
    """.replace("TOKEN", w_id)

    # launch the 4 queries separately and parse the queries
    # into a nicer dict via result_tojson.
    out1 = launch_sparql(query1, w_id)
    out2 = launch_sparql(query2, w_id)
    out3 = launch_sparql(query3, w_id)
    out4 = launch_sparql(query4, w_id)

    # parse the result into a single dict
    for o in [out1, out2, out3, out4]:
        for k, v in o.items():
            out[k] = v

    return out


def sparql():
    """
    launch queries on all wikidata ids stored in tables/wikidata_id.txt
    and save the result to out/wikidata/wikidata_enrichments.json
    :return: None
    """
    # parse the wikidata ids created at the end of itemtoid.py
    with open(f"{TABLES}/id_wikidata.txt", mode="r") as f:
        idlist = f.read().split()

    # create the output file and log file
    fp_out = f"{OUT}/wikidata/wikidata_enrichments.json"
    fp_log = f"{LOGS}/log_sparql.txt"
    if not os.path.isfile(fp_out):
        Path(fp_out).touch()
    if not os.path.isfile(fp_log):
        Path(fp_log).touch()

    # launch the query on all ids; if the output file is not empty,
    # read the contents and update it;
    # else, just write the contents
    for w_id in tqdm(idlist):
        log = open(fp_log, mode="r", encoding="utf-8")
        done = log.read().split()  # list of queried wikidata ids

        # manage the updates by checking that the id hasn't been queried
        if w_id not in done:
            done = []  # empty the long list of queried ids
            log.close()  # close the file to save memory
            with open(fp_out, mode="r+") as fh:
                if os.stat(fp_out).st_size > 0:
                    queried = json.load(fh)
                    queried[w_id] = config_sparql(w_id)
                    fh.seek(0)
                    json.dump(queried, fh, indent=4)
                else:
                    queried = {w_id: config_sparql(w_id)}
                    json.dump(queried, fh, indent=4)
                Logs.log_done(mode="sparql", data=w_id)
        else:
            log.close()

    return None


if __name__ == "__main__":
    sparql()
