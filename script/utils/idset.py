import csv

from .paths import TABLES, OUT


# -----------------------------------------------------------
# build a string of space separated deduplicated wikidata ids
# -----------------------------------------------------------


def build_idset():
    """
    build a list of wikidata ids without duplicates and write it to a file
    in order to launch sparql queries "en masse"
    :return: None
    """
    w_id = []  # list of wikidata ids
    out = ""  # string of space separated wikidata ids
    with open(f"{OUT}/wikidata/nametable_out.tsv", mode="r") as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            w_id.append(row[1])
    w_id = set(w_id)
    for w in w_id:
        out += f"{w} "
    with open(f"{TABLES}/id_wikidata.txt", mode="w") as f:
        f.write(out)
