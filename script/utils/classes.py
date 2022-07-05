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

        # print(input, "|", compa)
        same = (input == compa)  # true if same, false if not
        return same


class Errors:
    """
    error handling classes
    """
    @staticmethod
    def sparql_error_handle(query, w_id):
        """
        for sparql.py
        error handling for a wikidata sparql query
        :param query: the query on which an error happened
        :return: None
        """
        print(f"########### ERROR ON {w_id} ###########")
        error = traceback.format_exc()
        print("query text on which the error happened:")
        print(query)
        print(error)
        sys.exit(1)

    @staticmethod
    def itemtoid_error_handle(row, qdict):
        """
        for itemtoid.py
        error handling for itemtoid.py
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
