import json

# divide queried.json into smaller files by initials to save on ram. kind of a throwaway script

with open("logs/queried.json", mode="r") as f_in:
    queried = json.load(f_in)
    qdict = {}  # dictionnary to store the queried items
    for q in queried:
        if q[0] not in qdict.keys():
            qdict[q[0]] = {q: queried[q]}
        else:
            qdict[q[0]].update({q: queried[q]})

for q in qdict:
    with open(f"logs/queried_{q[0]}.json", mode="w") as f_out:
        json.dump(qdict[q], f_out, indent=4)

