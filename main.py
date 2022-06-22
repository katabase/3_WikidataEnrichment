import argparse
import sys

from script.sparql import launch
from script.itemtoid import itemtoid
from script.utils.traitcounter import counter
from script.itemtoid_test import itemtoid_test


# ---------------------------------------------------------
# command line interface script to decide which step to run
# ---------------------------------------------------------


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--test",
                        help="run a battery of tests (takes 20 minutes)",
                        action="store_true")
    parser.add_argument("-i", "--wikidataids",
                        help="get wikidata ids (takes up to 10 to 20 hours!)",
                        action="store_true")
    parser.add_argument("-c", "--traitcounter",
                        help="count the most used terms in the tei:trait (takes ~10 minutes)",
                        action="store_true")
    if len(sys.argv) == 1:
        sys.exit("""please enter the script to run:
            * -c --traitcounter : count most used terms in the tei:trait (to tweak the matching tables)
            * -t --test : run tests (takes ~20 minutes)
            * -i --get-wikidata-ids : retrieve wikidata ids (takes up to 10 to 20 hours!)
            * -s --run-sparql : run sparql queries (takes +-5 hours)
        """)
    parser.add_argument("-s", "--runsparql",
                        help="run sparql queries (takes +-5 hours)",
                        action="store_true")
    args = parser.parse_args()

    if args.test:
        itemtoid_test()
    elif args.wikidataids:
        itemtoid()
    elif args.runsparql:
        launch()
    elif args.traitcounter:
        counter()
