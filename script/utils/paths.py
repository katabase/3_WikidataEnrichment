import os

# path to important directories

ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir)
)  # root directory: 3_WikidataEnrichment/
OUT = os.path.join(ROOT, "out")  # final output directory
CATS = os.path.join(ROOT, "Catalogues")  # input catalogues
SCRIPT = os.path.join(ROOT, "script")
LOGS = os.path.join(SCRIPT, "logs")
TABLES = os.path.join(SCRIPT, "tables")
UTILS = os.path.join(SCRIPT, "utils")

