import re
import csv
import json

from .paths import TABLES


# -----------------------------------------------------------------
# counting the most frequent words in the tei:trait
# (can be used to update matching tables for new catalogue entries)
# -----------------------------------------------------------------


# ================= COUNT THE MOST FREQUENT WORDS ================= #
def counter():
    """
    get the most frequent words in tei:traits and the number of times they appear;
    write this dictionary in a json file ;
    used to get how to clusterize data
    :return: None
    """
    with open(f"{TABLES}/nametable_in.tsv", mode="r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        trait = ""
        for row in reader:
            trait += f"{row[3]} ; "
    traitlist = list(trait.split())  # total list of "words" in all the tei:trait

    # clean traitlist : remove punctuation
    cleanlist = []
    for t in traitlist:
        t = re.sub(r"\.|,|\(|\)", "", t)
        cleanlist.append(t)
    traitlist = cleanlist
    traitset = set(traitlist)  # list of unique items in traitlist

    # clean traitset : remove most frequent french characters and useless words
    # so that they aren't counted
    dellist = [".", " ", ";", ",", "-", "le", "la", "un", "une", "des", "de", "d'un", "d'une",
               "ce", "cette", "celui", "celle", "est", "a", "ses", "son", "sa", "leur",
               "leurs", "lui", "elle", "célèbre", "illustre", "homme", "femme", "par",
               "qui", "grand", "au", "fils", "plus", "moins", "les", "&", "é", "è", "et",
               "en", "m", "n", "fr", "du", "mort", "né", "morte", "née", "il", "elle",
               "eux", "avec", "puis", "fut", "vous", "l'illustre", "distingué", "savant",
               "sous", "fameux"]
    for d in dellist:
        if d in traitset:
            traitset.remove(d)
    counter = {}  # dictionary mapping to a word the number of times it is used

    # build counter
    nloop = 0
    print("beginning to count occurences of words")
    for w in traitset:
        nloop += 1
        if len(str(nloop)) >= 3 and str(nloop)[-2:] == "00":
            print(f"{nloop} out of {len(traitset)} words counted !")
        # we're looking for words to clusterize ; in turn, they must be meaningful traits, not numbers and such
        if not re.match(r"\d+", w) and not re.match("[A-Z]+", w):
            counter[w] = traitlist.count(w)

    # order counter by descending values
    counter_sort = {}
    for c in sorted(list(counter.values()), reverse=True):
        for k, v in counter.items():
            if v == c:
                counter_sort[k] = v
    # counter_sort = {k: v for k, v in sorted(counter.items(), key=lambda item: item[1])}
    # sortv = sorted(list(counter.values()))  # sorted count of words

    # save counter and print it
    with open(f"{TABLES}/wd_trait_wordcount.json", mode="w", encoding="utf-8") as out:
        json.dump(counter_sort, out, indent=4)
    print("done !")
    return None

if __name__ == "__main__":
    counter()