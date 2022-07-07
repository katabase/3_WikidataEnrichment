from SPARQLWrapper import XML
from lxml import etree
import traceback
import sys
import csv
import re

from .paths import LOGS


# ---------------------------------------------
# generic classes to hold some useful functions
# ---------------------------------------------


class Logs:
    """
    class to log aldready queried items in sparql.py and nametoid.py
    """
    @staticmethod
    def log_done(mode, data, orig=False, in_fpath=None):
        """
        for itemtoid.py and sparql.py
        write the aldready queried items to a log file (in order to avoid querying the same items again and again)
        - if used in orig=True mode, in_fpath must be supplied
        - if used in orig=False mode, data must be supplied
        :param mode: indicating the execution context (itemtoid.py or sparql.py).
                     possible values: itemtoid or sparql
        :param orig: to use only in itemtoid.py
                     boolean indicating that the log file is created for the first time: read all
                     of fpath and write it to the log file
        :param in_fpath: to use only with itemtoid.py
                         the input file path from which to get the queried entries when it's ran the first time
                         (should be ../out/wikidata/nametable_out.tsv)
        :param data: data to append to the log file if orig is False (data must be a queried entry's xml:id)
        :return: None
        """
        if mode == "itemtoid":
            fpath = f"{LOGS}/log_id.txt"
        else:
            fpath = f"{LOGS}/log_sparql.txt"
        with open(fpath, mode="a", encoding="utf-8") as f_out:
            if orig is True:  # for mode=itemtoid only
                with open(in_fpath, mode="r", encoding="utf-8") as f_in:
                    in_reader = csv.reader(f_in, delimiter="\t")
                    for row in in_reader:
                        f_out.write(f"{row[0]} ")  # write the entry's xml:id to log_done.txt
            else:
                f_out.write(f"{data} ")
        return None


class Strings:
    """
    class for basic functions on strings: cleaning, comparing
    """
    @staticmethod
    def striptag(instr):
        """
        for itemtoid.py
        strip html tags returned by the wikidata api
        :param instr: input string to strip tags from
        :return: the string with stripped tags
        """
        outstr = re.sub(r"<.*?>", "", instr)
        return outstr

    @staticmethod
    def clean(s):
        """
        for sparql.py
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

    @staticmethod
    def compare(input, compa):
        """
        for sparql.py
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
        same = (input == compa)  # true if same, false if not
        return same


class Converters:
    """
    conversion from one datatype to another
    """
    @staticmethod
    def xmltojson(in_xml):
        """
        convert a sparql query returned in xml format to json. the results are
        returned in a sparql-like json to be later converted to the json we use
        using sparql.py/result_tojson()
        :return: a sparql-like json (see sparql.py/sparql documentation)
        """
        ns = {"sparql": "http://www.w3.org/2005/sparql-results#"}
        out_json = {}  # output dictionary

        # parse the xml
        tree = etree.fromstring(in_xml)
        vars = tree.xpath(".//sparql:head//sparql:variable/@name", namespaces=ns)  # all queried variables
        results = tree.xpath(".//sparql:results/sparql:result", namespaces=ns)  # all results

        # build the header + body base of the output json
        out_json["head"] = {"vars": vars}
        out_json["results"] = {"bindings": []}

        # built the body of the output json. r = sparql:result, or a series of values of var mapped to 1 result
        for r in results:
            result = {}  # sparql:results are expressed in json as dicts inside the out_json["results"]["bindings"] list
            for v in vars:
                try:
                    result[v] = {
                        # type = xml binding child element's name. we strip the namespace tag
                        "type": r.xpath(
                            f"./sparql:binding[@name='{v}']//*", namespaces=ns
                        )[0].tag.replace("{http://www.w3.org/2005/sparql-results#}", ""),
                        # value = xml binding child element's content
                        "value": r.xpath(f"./sparql:binding[@name='{v}']//*", namespaces=ns)[0].text
                    }
                    # datatype = xml binding child element's @datatype attribute. the datatype is not always included
                    if len(r.xpath(f"./sparql:binding[@name='{v}']//*/@datatype", namespaces=ns)) > 0:
                        result["datatype"] = r.xpath(f"./sparql:binding[@name='{v}']//*/@datatype", namespaces=ns)
                except IndexError:
                    pass
            out_json["results"]["bindings"].append(result)

        return out_json

    @staticmethod
    def result_tojson(wd_result):
        """
        transform the JSON returned by wikidata into a more elegant JSON
        (mapping to a query variable a list of results (empty list if nothing is returned
        by sparql. see the documentation for the above function for details))
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


class ErrorHandlers:
    """
    error handling classes
    """
    @staticmethod
    def sparql_general(query, w_id):
        """
        for sparql.py
        general error handling for a wikidata sparql query;
        prints the actual sparql query and exits the script.
        :param query: the query on which an error happened
        :param w_id: the wikidata id currently queried
        :return: None
        """
        print(f"########### ERROR ON {w_id} ###########")
        error = traceback.format_exc()
        print("query text on which the error happened:")
        print(query)
        print(error)
        sys.exit(1)

    @staticmethod
    def sparql_incomplete_read(endpoint, query):
        """
        for sparql.py
        error handling if there's a json parse xml error:
        relaunch the query with xml result format
        and convert it to sparql-like json (aka, a json that follows the sparql specification:
        https://www.w3.org/TR/2013/REC-sparql11-overview-20130321/).
        don't exit the script afterwards.
        :param endpoint: the sparql endpoint used
        :param query: the current sparql query
        :return: out, the query results returned to json
        """
        # if there's a json parse xml error,
        endpoint.setQuery(query)
        endpoint.setReturnFormat(XML)
        results = endpoint.queryAndConvert()
        results = Converters.xmltojson(str(results))
        out = Converters.result_tojson(results)
        return out

    @staticmethod
    def sparql_internal_error(query):
        """
        for sparql.py
        internal errors = http code 500, usually because of a timeout.
        in that case, build an empty result list and return the result.
        don't exit the script afterwards.
        :return: out, a dict mapping to queried variables empty lists
        """
        out = {}
        vars = re.search(r"SELECT DISTINCT ((\?\w*|\s)*)", query)[1].split()  # list of queried variables
        for v in vars:
            out[v.replace("?", "")] = []
        return out

    @staticmethod
    def itemtoid(row, qdict):
        """
        for itemtoid.py
        error handling for itemtoid.py. ends the script.
        :param row: the csv row on which there is an error
        :param qdict: the query dictionnary on which there's an error
        :return:
        """
        print(f"########### ERROR ON {row[0]} ###########")
        print(row)
        print(qdict)
        error = traceback.format_exc()
        print(error)
        sys.exit(1)


r1 = """<?xml version="1.0" ?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
	
	
	<head>
		
		
		<variable name="id"/>
		
		
		<variable name="labelen"/>
		
		
		<variable name="url"/>
		
		
		<variable name="collid"/>
		
		
		<variable name="colllabel"/>
		
		
		<variable name="img"/>
		
		
		<variable name="idisni"/>
		
		
		<variable name="idviaf"/>
		
		
		<variable name="idbnf"/>
		
		
		<variable name="idcongress"/>
		
		
		<variable name="idartsy"/>
		
	
	</head>
	
	
	<results>
		
		
		<result>
			
			
			<binding name="labelen">
				
				
				<literal xml:lang="en">Joana Hadjithomas</literal>
				
			
			</binding>
			
			
			<binding name="id">
				
				
				<literal>Q3179639</literal>
				
			
			</binding>
			
			
			<binding name="img">
				
				
				<uri>http://commons.wikimedia.org/wiki/Special:FilePath/Joanna%20hadhjithomas.jpg</uri>
				
			
			</binding>
			
			
			<binding name="idisni">
				
				
				<literal>0000 0001 2132 1223</literal>
				
			
			</binding>
			
			
			<binding name="idviaf">
				
				
				<literal>49362327</literal>
				
			
			</binding>
			
			
			<binding name="idbnf">
				
				
				<literal>13169967x</literal>
				
			
			</binding>
			
			
			<binding name="idcongress">
				
				
				<literal>nr2006017200</literal>
				
			
			</binding>
			
		
		</result>
		
		<result>
			
			
			<binding name="collid">
				
				
				<uri>http://www.wikidata.org/entity/Q924335</uri>
				
			
			</binding>
			
			
			<binding name="colllabel">
				
				
				<literal xml:lang="en-gb">Stedelijk Museum Amsterdam</literal>
				
			
			</binding>
			
			
			<binding name="labelen">
				
				
				<literal xml:lang="en">Mohamed Bourouissa</literal>
				
			
			</binding>
			
			
			<binding name="id">
				
				
				<literal>Q3318456</literal>
				
			
			</binding>
			
			
			<binding name="idisni">
				
				
				<literal>0000 0003 6263 911X</literal>
				
			
			</binding>
			
			
			<binding name="idviaf">
				
				
				<literal>225099587</literal>
				
			
			</binding>
			
			
			<binding name="idbnf">
				
				
				<literal>15744950w</literal>
				
			
			</binding>
			
			
			<binding name="idcongress">
				
				
				<literal>n2014015573</literal>
				
			
			</binding>
			
		
		</result>
	
	</results>
	

</sparql>
"""

r2 = """<?xml version="1.0" ?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
	
	
	<head>
		
		
		<variable name="id"/>
		
		
		<variable name="labelen"/>
		
		
		<variable name="url"/>
		
		
		<variable name="collid"/>
		
		
		<variable name="colllabel"/>
		
		
		<variable name="img"/>
		
		
		<variable name="idisni"/>
		
		
		<variable name="idviaf"/>
		
		
		<variable name="idbnf"/>
		
		
		<variable name="idcongress"/>
		
		
		<variable name="idartsy"/>
		
	
	</head>
	
	
	<results>
		
		
		<result>
			
			
			<binding name="collid">
				
				
				<uri>http://www.wikidata.org/entity/Q924335</uri>
				
			
			</binding>
			
			
			<binding name="colllabel">
				
				
				<literal xml:lang="en-gb">Stedelijk Museum Amsterdam</literal>
				
			
			</binding>
			
			
			<binding name="labelen">
				
				
				<literal xml:lang="en">Mohamed Bourouissa</literal>
				
			
			</binding>
			
			
			<binding name="id">
				
				
				<literal>Q3318456</literal>
				
			
			</binding>
			
			
			<binding name="idisni">
				
				
				<literal>0000 0003 6263 911X</literal>
				
			
			</binding>
			
			
			<binding name="idviaf">
				
				
				<literal>225099587</literal>
				
			
			</binding>
			
			
			<binding name="idbnf">
				
				
				<literal>15744950w</literal>
				
			
			</binding>
			
			
			<binding name="idcongress">
				
				
				<literal>n2014015573</literal>
				
			
			</binding>
			
		
		</result>
		
	
	</results>
	

</sparql>
"""
