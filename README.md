# LEVEL 3 - WIKIDATA ENRICHMENTS

---

## Presentation

This part of the pipeline
- reconciles the names in our dataset with wikidata IDs
- runs the same 4 `sparql` requests on all IDs
- stores the output of the `sparql` requests in a `sparql` file
- updates the `tei` files with the wikidata IDs

The aim is to produce normlised data to connect to catalogue entries, in
order to understand our dataset better and to isolate the factors detrmining
a price.

---

## Installation, pipeline and use

---

### Installation

This works on MacOS and Linux (ubuntu, debian based distributions).
```shell
git clone https://github.com/katabase/3_WikidataEnrichment  # clone the repo
cd 3_WikidataEnrichment  # move to the dictory
python3 -m venv env  # create a python virtualenv
source env/bin/activate  # source python from the virtualenv
pip install -r requirements.txt  # install the necessary librairies
```

---

### Pipeline and use

All scripts run by running `main.py` with a specific argument. 4 GBs of RAM are recommended to
run the scripts. 

As a reminder, here is catalogue entries'`tei` 
structure:

```xml
<item n="80" xml:id="CAT_000146_e80">
   <num>80</num>
   <name type="author">Cherubini (L.),</name>
   <trait>
      <p>l'illustre compositeur</p>
   </trait>
   <desc>
      <term>L. a. s.</term>;<date>1836</date>,
      <measure type="length" unit="p" n="1">1 p.</measure> 
      <measure unit="f" type="format" n="8">in-8</measure>.
      <measure commodity="currency" unit="FRF" quantity="12">12</measure>
    </desc>
</item>
```

---

**Step 1 : create an input TSV** - `python main.py -n`

The first step is to create a `tsv` file that will be used to retrieve the wikidata IDs:
- the `tsv` is made of 5 columns (see example below):
    - `xml id` : the item's `xml:id`
    - `wikidata id` : the wikidata ID (to be retrieved in the next step)
    - `name` : the `tei:name` of that item
    - `trait` : the `tei:trait` of that item
```csv
xml id,wikidata id,name,trait
CAT_000362_e27086,,ADAM (Ad.),célèbre compositeur de musique.
```  
- **running this step**:
```shell
python main.py -n
```

---

**Step 2 : retrieve the wikidata IDs** - `python main.py -i`

The wikidata IDs are retrieved by running a full text search using the 
[wikidata API](https://www.wikidata.org/w/api.php). 
- the **algorithm functions** as follows:
    - the input is file created at the previous step (`script/tables/nametable_in.tsv`).
      The `name` and `trait` columns are used to create data for the API search
    - two columns are processed to prepare the data for the API search:
        - from the `name`, we determine the kind of `name` we're working with
          (the name of a person, of a nobility, of an event, of a place...). This
          determines different behaviours.
        - the `name` is normalized: we extract and translate nobility titles, locations...
          First and last names are extracted. If the first name is abbreviated, we try to
          rebuild a full name from its abbreviated version.
        - the `trait` is processed to extract and translate occupations, dates...
        - the output is stored in a dictionnary
    - this `dict` is passed to a second algorithm to run text searches on the API. Depending on
      the data stored in the dict, different queries are ran. A series of queries are run until
      a result is obtained
    - finally, the result is written to a TSV file (`out/wikidata/nametable_out.tsv`). Its structure
      is the same as that of `nametable_in`, with some changes. Here are the column names:
        - `tei:xml_id` : the `@xml:id` from the `tei` files
        - `wd:id` : the wikidata ID
        - `tei:name` : the `tei:name`
        - `wd:name` : the name corresponding to the wikidata ID (to ease the data verification process)
        - `wd:snippet` : a short summary of the wikidata page (to ease the data verification process)
        - `tei:trait` : the `tei:trait`
        - `wd:certitude` : an evaluation of the degree of certitude (whether we're certain that the proper
           id has been retrieved)
    - once this script has completed, a deduplicated list of wikidata IDs is written to `script/tables/id_wikidata.txt`.
      This file will be used as input for the next step.
    - the F1 score for this step (evaluating the number of good wikidata IDs retrieved) is `0.674`,
      based on tests run on 200 items.
    - this step takes a lot of time to complete, but, thanks to log files, the script can be interrupted and
      restarted at any point.
- **running this step** : 
```shell
python main.py -i
```

---

**Step 3 : running `sparql` queries** - `python main.py -s`
- the **algorithm** is much simpler: for each wikidata ID, 4 sparql queries are run. The results are returned
  in `json` or, if there's a mistake, `xml`. The results are translated to a simpler `json` and the result is stored to
  `out/wikidata/wikidata_enrichments.json`. This step takes a lot of time, but the script can be stopped and continued
  at any point.
- the **output structure** is as follows (each key is mapped to a list of results ; the list can be empty ;
  the empty lines in the dict separates the different wikidata queries):
```python
    out = {
        'instance': [], 'instanceL': [],     # what "category" an id belongs to (person, litterary work...)
        'gender': [], 'genderL': [],     # the gender of a person
        'citizenship': [], 'citizenshipL': [],     # citizenship
        'lang': [], 'langL': [],     # languages spoken
        'deathmanner': [], 'deathmannerL': [],     # the way a person died
        'birthplace': [], 'birthplaceL': [],     # the place a person is born
        'deathplace': [], 'deathplaceL': [],     # the place a person died
        'residplace': [], 'residplaceL': [],     # the place a person lived
        'burialplace': [], 'burialplaceL': [],     # where a person is buried
        
        'educ': [], 'educL': [],     # where a person studied
        'religion': [], 'religionL': [],     # a person's religion
        'occupation': [], 'occupationL': [],     # general description of a person's occupation
        'award': [], 'awardL': [],     # awards gained
        'position': [], 'positionL': [],     # precise positions held by a person
        'member': [], 'memberL': [],     # institution a person is member of
        'nobility': [], 'nobilityL': [],   # nobility titles
        'workcount': [],     # number of works (books...) documented on wikidata
        'conflictcount': [],     # number of conflicts (wars...) a person has participated in
        'image': [],     # url to the portrait of a person
        'signature': [],    # url to the signature of a person
        'birth': [], 'death': [],     # birth and death dates
        'title': [],     # title of a work of art / book...
        'inception': [],     # date a work was created or published
        'author': [], 'authorL': [],     # author of a book
        'pub': [], 'pubL': [],     # publisher of a work
        'pubplace': [], 'pubplaceL': [],     # place a work was published
        'pubdate': [],     # date a work was published
        'creator': [], 'creatorL': [],     # creator of a work of art
        'material': [], 'materialL': [],     # material in which a work of art is made
        'height': [],     # height of a work of art
        'genre': [], 'genreL': [],     # genre of a work or genre of works created by a person
        'movement': [], 'movementL': [],     # movement in which a person or an artwork are inscribed
        'creaplace': [], 'creaplaceL': [],     # place where a work was created
        'viafID': [],     # viaf identifier
        'bnfID': [],     # bibliothèque nationale de france ID
        'isniID': [],      # isni id
        'congressID': [],      # library of congress identifier
        'idrefID': []      # idref identifier
    }
```
- **running this step**:
```shell
python main.py -s
```

---

**Running tests** - `python main.py -t`
- the tests are only run on the **step 2** (for the rest, we are certain of the result). 
    - They are based on 200 catalogue entries. The test dataset ressembles the full dataset (about as many
      different kinds of entries, from different catalogues, with as many `tei:trait`s as in the main dataset)
    - Several tests are run. Two tests are testing isolate parameters of the dictionnary built in the step 1 and the
      efficiency of the function that rebuilds the first name from its abbreviation. The other tests
      are for the final algorithm and they build statistics it. They also calculate its execution time
      using different parameters.
- **running the tests**:
```shell
python main.py -t
```

---

**Other options**:
- **counting the most used words in the `tei:trait`s** of the input dataset (to tweak the way the dictionnary is
  built in the step 2) : `python main.py -c`
- **`python main.py -x`** : a throwaway option to map to a function in order to use a script that is not accessible
  from the above arguments

---

**Summarizing, the options are**
```
* -c --traitcounter : count most used terms in the tei:trait (to tweak the matching tables)
* -t --test : run tests (takes ~20 minutes)
* -i --wikidataids : retrieve wikidata ids (takes up to 10 to 20 hours!)
* -s --runsparql : run sparql queries (takes +-5 hours)
* -n --buildnametable: build the input table for -i --wikidataids (a table from which 
                       to retrieve wikidata ids
* -x --throwaway : run the current throwaway script (to test a function or whatnot)
```

---

## Credits

Scripts developped by Paul Kervegan in spring-summer 2022 and available under GNU-GPL 3.0 license.
