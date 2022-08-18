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


def wd_2_tei(tree: etree._ElementTree, nametable: _csv.reader):
    """
    link a wikidata identifier to a catalogue entry's tei:name
    :param tree: an lxml tree of the catalogue being processed
    :param nametable: the nametable parsed in pase_nametable
    :return: the updated catalogue
    """
    for row in nametable:
        for tei_name in tree.xpath(".//tei:body//tei:name", namespaces=ns):
            if tei_name.text == row[2]:
                tei_name.set("ana", row[1])
    return tree


def cat_processor():
    """
    loop on all catalogues, parse them, call wd_2_tei and
    save the output to out/catalogues
    :return:
    """
    parser = etree.XMLParser(remove_blank_text=True)
    fh_nametable = open(f"{OUT}/wikidata/nametable_out.tsv", mode="r")
    nametable = csv.reader(fh_nametable, delimiter="\t")
    logpath = f"{LOGS}/log_wd2tei.txt"
    outpath = f"{OUT}/catalogues/"

    # create files and dirs
    if not os.path.isdir(outpath):
        os.makedirs(outpath)
    if not os.path.isfile(logpath):
        print("hi1")
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

                    # rollback csv to its beginning and skip headers;
                    # parse xml contents
                    fh_nametable.seek(0)
                    next(fh_nametable)
                    tree = etree.parse(fh, parser=parser)

                # process the files
                tree = wd_2_tei(tree, nametable)
                etree.indent(tree, space="    ")

                # write output and write the catalogue's id to the log file
                with open(outfile, mode="w+") as fh:
                    fh.write(str(etree.tostring(
                        tree, xml_declaration=True, encoding="utf-8", pretty_print=True
                    ).decode("utf-8")))
                Logs.log_done(mode="wd2tei", orig=False, data=catid)

    fh_nametable.close()
    return None
