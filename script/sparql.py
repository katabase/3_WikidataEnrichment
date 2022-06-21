from SPARQLWrapper import SPARQLWrapper, JSON, CSV
from tqdm import tqdm
import traceback
import sys

from idset import build_idset

# --------------------------------------------------
# launch sparql queries and save the result to a csv
# --------------------------------------------------


def sparql(w_id):
    """
    launch a sparql query and return the result
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
            done = {}  # dictionary to contain the processed results

            # build done, a
            for v in var:
                key = v.replace("L", "")

                # if there's a result for that request
                if v in bind:
                    # remove possible url to keep only id
                    data = stripurl(bind[v]["value"])

                    # if the result variable is a label
                    if v[-1] == "L":
                        if key not in done:
                            done[key] = ["", data]
                        else:
                            done[key].append(data)

                    # if there's a label associated to that result variable
                    elif f"{v}L" in var:
                        if key not in done:
                            done[key] = [data]
                        else:
                            done[key][0] = data

                    # if there's no label (basically, if it's a birth/death date)
                    else:
                        done[key] = [data]

                    # create the key for out if it hasn't been created yet
                    """if key not in out:
                        out[key] = []
                    if v[-1] == "L":
                    print(v)"""

                # if there's no result for that request
                else:
                    done[key] = []

            # print(done)

            # build out from the content of done
            # CA BUGUE
            for k, v in done.items():
                if k not in out.keys():
                    if len(v) > 0:
                        out[k] = [v]
                    else:
                        out[k] = []
                elif k in out.keys() and len(v) > 0 \
                        and not any([v] == o for o in list(out.values())):
                    out[k].append(v)

        print(out)

        # print(results)
        
    except:
        print(f"########### ERROR ON {w_id} ###########")
        error = traceback.format_exc()
        print("query text on which the error happened:")
        print(query)
        print(error)
        sys.exit(1)


def launch():
    """
    launch queries on all wikidata ids stored in tables/wikidata_id.txt
    and save the result to out/wikidata/_____NAME TO DEFINE_____
    :return:
    """
    # build a unique list of ids and save it to a file
    build_idset()

    # read wikidata ids and launch queries
    with open("tables/id_wikidata.txt", mode="r") as f:
        idlist = f.read().split()
    print(idlist[0])
    for w_id in tqdm(idlist):
        sparql(w_id)


def stripurl(url):
    """
    strip the beginning of a wikidata url in order
    to keep only the id : http://www.wikidata.org/entity/
    :param url: the input string: a wikidata url
    :return: url, the id alone.
    """
    url = url.replace("http://www.wikidata.org/entity/", "")
    return url


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