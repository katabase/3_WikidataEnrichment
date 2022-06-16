# LEVEL 3 - WIKIDATA ENRICHMENTS

---

## Presentation

This part of the pipeline
- reconciles the names in our dataset with wikidata IDs, 
- updates the TEI files with wikidata IDs
- runs the same SPARQL request on all IDs
- stores the output of SPARQL requests in a JSON file.

The aim is to produce normlised data to connect to catalogue entries, in
order to understand our dataset better and to isolate the factors detrmining
a price.

---

## Pipeline

The first step is to create a TSV file that will be used to retrieve the wikidata IDs
(`nametable.py`)

---

## Credits

Scripts developped by Paul Kervegan in spring-summer 2022.
