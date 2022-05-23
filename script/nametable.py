import glob
from lxml import etree
import csv
import os
import re

ns = {"tei": "http://www.tei-c.org/ns/1.0"}  # tei namespace


def csvbuilder():
    """
    build a csv to link the persons in the dataset to a wikidata id.
    basic structure of the csv:
    || item xml:id || person's wikidata id || person's name || person's tei:trait

    in some items, the there are more than 1 tei:trait, but not the same number of
    tei:trait as there are of tei:name; in this case, it is impossible to match a person
    to their trait. in turn,
    - instead of a person's name (column 3), there is a list of the names in the item
    - instead of a person's tei: trait (column 4), there is a list of the traits in the item.
    in that case, the sub-separator in columns 3 and 4 is ";". the data structure is as follows:
    || item xml:id || person's wikidata id || list of tei:names in the item || list of tei:traits in the item.

    the csv is saved in tables/.

    :return:
    """
    input_f = glob.glob(os.path.abspath(
        os.path.join(os.getcwd(), os.pardir, "Catalogues", "*0*", "CAT_*.xml")
    ))  # get the input files
    idwiki = ""  # empty wikipedia identifier
    if not os.path.isdir(os.path.join(os.getcwd(), "tables")):
        os.makedirs("tables")
    with open("tables/nametable_in.tsv", mode="w", encoding="utf-8") as out:
        writer = csv.writer(out, delimiter="\t", quotechar="\"")
        writer.writerow(["xml id", "wikidata id", "name", "trait"])
        for f in input_f:
            tree = etree.parse(f)
            for item in tree.xpath("//tei:body//tei:item", namespaces=ns):
                if item.xpath("@xml:id", namespaces=ns):
                    xmlid = item.xpath("@xml:id", namespaces=ns)[0]
                if item.xpath("./tei:name/text()", namespaces=ns):
                    name = item.xpath("./tei:name/text()", namespaces=ns)
                if item.xpath("./tei:trait//*", namespaces=ns):
                    trait = item.xpath("./tei:trait//*/text()", namespaces=ns)

                # write the rows in the csv
                # - if there are no tei:traits, an empty string is added instead of the tei:traits (case 1)
                # - if there are as many tei:trait as tei:name, we consider that a trait is associated
                #   to a name; in that case, one name is linked to its corresponding trait in each csv row;
                #   (case 4 and 5)
                # - if there's only one trait but several names, we consider that the name belongs to
                #   the first tei:name (note that this happens extremely rarely) (case 3)
                # - else, there is a different number of tei:names and tei:traits in the tei:item, but
                #   more than 1 tei:trait. we can't determine the link between name and trait (note that it's the
                #   case rarely, if ever). in that case, a csv row is composed of an item's xml id, an empty
                #   wikidata id, a list of all the tei:names in the item, a list of all the tei:traits in the items.
                #   the ";" subseparator is used in that case. (case 2)
                if len(trait) == 0:
                    if len(name) == 1:
                        writer.writerow([xmlid, idwiki, re.sub(r"\s+", " ", name[0].replace("\n", "")), ""])
                    else:
                        names = ""
                        nloop = 0
                        for n in name:
                            nloop += 1
                            names += re.sub(r"\s+", " ", n.replace("\n", ""))
                            if nloop < len(name):
                                names += "; "
                        writer.writerow([xmlid, idwiki, names, ""])
                elif len(trait) != len(name) and len(trait) > 1:
                    traits = ""
                    nloop = 0
                    for t in trait:
                        nloop += 1
                        traits += re.sub(r"\s+", " ", t.replace("\n", ""))
                        if nloop < len(trait):
                            traits += "; "  # add a sub separator
                    names = ""
                    nloop = 0
                    for n in name:
                        nloop += 1
                        names += re.sub(r"\s+", " ", n.replace("\n", ""))
                        if nloop < len(name):
                            names += "; "
                    writer.writerow([xmlid, idwiki, names, traits])
                elif len(trait) != len(name) and len(trait) == 1:
                    nloop = 0
                    for n in name:
                        nloop += 1
                        if nloop == 1:
                            writer.writerow([xmlid, idwiki, n, trait[0]])
                        else:
                            writer.writerow([xmlid, idwiki, n, ""])
                elif len(trait) == len(name) > 1:
                    mapping = zip(name, trait)
                    for m in mapping:
                        writer.writerow([xmlid, idwiki, re.sub(r"\s+", " ", m[0].replace("\n", "")),
                                        re.sub(r"\s+", " ", m[1].replace("\n", ""))])
                else:  # if there is only 1 desc and name
                    writer.writerow([xmlid, idwiki, re.sub(r"\s+", " ", name[0].replace("\n", "")),
                                    re.sub(r"\s+", " ", trait[0].replace("\n", ""))])

                # reinitialise the values to avoid values from previous items to be added
                # to new rows; else, if there's a tei:desc for an element A but not for
                # an element B, A's tei:desc is added to the csv row corresponding to B
                xmlid = ""
                name = []
                trait = []

    return None


if __name__ == "__main__":
    csvbuilder()
