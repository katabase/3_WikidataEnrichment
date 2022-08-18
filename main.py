import argparse
import sys

from script.sparql import sparql
from script.itemtoid import itemtoid
from script.utils.nametable import csvbuilder
from script.utils.traitcounter import counter
from script.itemtoid_test import itemtoid_test
from script.wd2tei import cat_processor


# ---------------------------------------------------------
# command line interface script to decide which step to run
# ---------------------------------------------------------


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--wikidataids",
                        help="get wikidata ids (takes up to 10 to 20 hours!)",
                        action="store_true")
    parser.add_argument("-s", "--runsparql",
                        help="run sparql queries (takes +-5 hours)",
                        action="store_true")
    parser.add_argument("-w", "--wdtotei",
                        help="link the wikidata identifiers to the tei:names in the catalogues",
                        action="store_true")
    parser.add_argument("-t", "--test",
                        help="run a battery of tests on -i --wikidataids (takes 20 minutes)",
                        action="store_true")
    parser.add_argument("-n", "--buildnametable",
                        help="build the input table for -i --wikidataids",
                        action="store_true")
    parser.add_argument("-c", "--traitcounter",
                        help="count the most used terms in the tei:trait (takes ~10 minutes)",
                        action="store_true")
    parser.add_argument("-x", "--throwaway",
                        help="throwaway command to test functions and such. uses can change.",
                        action="store_true")
    if len(sys.argv) == 1:
        sys.exit("""please enter the script to run:
            * -c --traitcounter : count most used terms in the tei:trait (to tweak the matching tables)
            * -t --test : run tests (takes ~20 minutes)
            * -i --wikidataids : retrieve wikidata ids (takes up to 10 to 20 hours!)
            * -s --runsparql : run sparql queries (takes +-5 hours)
            * -w --wdtotei : link the wikidata identifiers to the tei:names in the catalogues
            * -n --buildnametable: build the input table for -i --wikidataids (a table from which 
                                    to retrieve wikidata ids)
            * -x --throwaway : run the current throwaway script (to test a function or whatnot)
        """)
    args = parser.parse_args()

    if args.test:
        itemtoid_test()
    elif args.wikidataids:
        itemtoid()
    elif args.runsparql:
        sparql()
    elif args.wdtotei:
        cat_processor()
    elif args.traitcounter:
        counter()
    elif args.buildnametable:
        csvbuilder()
    elif args.throwaway:
        print("the throwaway argument is currently not mapped to any function.")

