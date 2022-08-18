from lxml import etree
from tqdm import tqdm
import glob
import _csv
import csv
import os
import re

from .utils.paths import CATS, OUT, LOGS
from .utils.classes import Logs


# --------------------------------------------------------------
# link wikidata ids to the tei:names in an @ana
# and update the catalogues
# --------------------------------------------------------------


ns = {"tei": "http://www.tei-c.org/ns/1.0"}


def build_mapper():
    """
    create a dict mapping to a tei:name a wikidata id
    :return: mapper, a dict with this structure: {"tei:name": "wikidata id"}
    """
    with open(f"{OUT}/wikidata/nametable_out.tsv", mode = "r") as fh:
        nametable = csv.reader(fh, delimiter="\t")
        next(fh)  # skip headers
        mapper = {row[2]: row[1] for row in nametable}
    return mapper


def wd_2_tei(tree: etree._ElementTree, mapper: dict):
    """
    link a wikidata identifier to a catalogue entry's tei:name
    :param tree: an lxml tree of the catalogue being processed
    :param mapper: a dict mapping to a tei:name a wikidata ID
    :return: the updated catalogue
    """
    for tei_name in tree.xpath(".//tei:body//tei:name", namespaces=ns):
        try:
            tei_name.set("ana", mapper[tei_name.text])
        except KeyError:
            pass
    return tree


def cat_processor():
    """
    loop on all catalogues, parse them, call wd_2_tei and
    save the output to out/catalogues.
    there are logs used because the original version took much longer,
    but it is now much quicker
    :return: None
    """
    parser = etree.XMLParser(remove_blank_text=True)
    logpath = f"{LOGS}/log_wd2tei.txt"
    outpath = f"{OUT}/catalogues/"
    mapper = build_mapper()

    # create files and dirs
    if not os.path.isdir(outpath):
        os.makedirs(outpath)
    if not os.path.isfile(logpath):
        with open(logpath, mode="w") as logfile:
            logfile.write("")

    # get the output directory names from the input dirs;
    # create the output dirs
    for catdir in glob.glob(f"{CATS}/*"):

        catcode = re.search(r"\d+-\d+$", catdir)[0]  # 1-100, 201-300...
        outdir = f"{outpath}{catcode}_wd"
        if not os.path.isdir(outdir):
            os.makedirs(outdir)
        print(f"working on catalogues: {catcode}")

        # read each file, process it and write it to output
        for cat in tqdm(
                glob.glob(f"{catdir}/*.xml"),
                desc="adding the wikidata identifiers to the xml files",
                total=len(glob.glob(f"{catdir}/*.xml"))
        ):
            catid = re.search(r"CAT_\d+", cat)[0]  # current catalogue's @xml:id
            outfile = f"{outdir}/{catid}_wd.xml"  # path to the output file

            # check that a catalogue hasn't been processed
            log = open(logpath, mode="r", encoding="utf-8")
            done = log.read().split()

            # if it hasn't been processed, then it's time to do it
            if catid not in done:
                with open(cat, mode="r") as fh:
                    tree = etree.parse(fh, parser=parser)

                # process the files
                tree = wd_2_tei(tree, mapper)
                etree.indent(tree, space="    ")

                # write output and write the catalogue's id to the log file
                with open(outfile, mode="w+") as fh:
                    fh.write(str(etree.tostring(
                        tree, xml_declaration=True, encoding="utf-8", pretty_print=True
                    ).decode("utf-8")))
                Logs.log_done(mode="wd2tei", orig=False, data=catid)

    return None
