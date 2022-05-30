import re

from tables.matching import comp_names, names


# ================= WORKING ON NAMES: REGEX MATCHING AND GETTING THE FULL 1ST NAME ================= #
def rgx_abvcomp(nstr):
    """
    try to extract an abbreviated composed first name. if there is no match, return None

    pattern
    -------
    the patterns in the example below are simplified to keep things readable
    - two strings separated by a "-" or "\s"
    - the first or second string can be a full name ([A-Z][a-z]+)
      or an abbreviation ([A-Z][a-z]*\.)
    - if the strings are separated by "\s", they must be finished by "\."
      (to be sure that we don't capture full names, i.e: "J. Ch."  can be captured,
      but not "Jean Charles")
    - complex names with 3 or more words must have "-" and at least one "\."
    - (\s|$) and (^|\s) are safeguards to avoid matching the end or beginning of another word

    examples
    --------
    matched : M.-Madeleine Pioche de la Vergne  # matched string : M.-Madeleine
    matched : C.-A. de Ferriol  # matched string : C.-A.
    matched : J. F.  # matched string : J. F.
    matched : Jean F.  # matched string : Jean F.
    matched : Jean-F.  # matched string : Jean-F.
    matched : A M  # matched string : A M
    matched : C.-Edm.-G.  # matched string : C.-Edm.-G.
    matched : Charles-Edm.-G.  # matched string : Charles-Edm.-G.
    not matched : Anne M
    not matched : Claude Henri blabla
    not matched : Claude Henri

    :param nstr: the name string used as input
    :return: the matched string if there is a match ; None if there is no match
    """
    mo = re.search(r"(^|,|\s)[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]*"
                   + "\.?-[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]*\.(\s|,|$)", nstr) \
         or re.search(r"(^|,|\s)[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]*\."
                      + "-[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]*\.?(\s|,|$)", nstr) \
         or re.search(r"(^|,|\s)[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ]\.?\s"
                      + "[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]*\.(\s|,|$)", nstr) \
         or re.search(r"(^|,|\s)[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]*\.?"
                      + "\s[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ]\.(\s|,|$)", nstr) \
         or re.search(r"(^|,|\s)[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ]\.?\s[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ]\.?(\s|,|$)", nstr) \
         or re.search(r"([A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ]\.){2,}", nstr) \
         or re.search(r"(^|,|\s)([A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]*\.?-)+"
                      + "([A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]*\.)(\s|,|$)", nstr) \
         or re.search(r"(^|,|\s)([A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]*\.-)+"
                      + "([A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]*\.?)(\s|,|$)", nstr)
    if mo is not None:
        return mo[0]
    else:
        return None


def rgx_abvsimp(nstr):
    """
    try to extract a "simple" (not composed) abbreviated first name. if there is no match, return None

    pattern
    -------
    a capital letter (possibly followed by a certain number of lowercase letters)
    ended with a dot. (\s|$) and (^|\s) are safeguards to avoid matching the beginning
    end of another word.
    *warning* : it can also capture parts of composed abbreviated names => must be used
    in an if-elif after trying to match a composed abbreviated name

    examples
    --------
    matched : bonjour Ad.  # matched string : Ad.
    matched : J. baronne  # matched string : J.
    matched : J. F.  # matched string : J.
    matched : Jean F.  # matched string : F.
    not matched : A.-M.
    not matched : Anne M
    not matched : Hector

    :param nstr: the name string used as input
    :return:
    """
    mo = re.search(r"(^|\s)[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]*\.(\s|$|,)", nstr)
    if mo is not None:
        return mo[0]
    else:
        return None


def rgx_complnm(nstr):
    """
    try to extract a complete name from a string. if there is no match, return None

    pattern
    -------
    - an uppercase letter followed by several lowercase letters ;
    - this pattern can be repeated several times, separated by a space or "-"
    - (\s|$) and (^|\s) are safeguards to avoid matching the beginning or end of another word.

    :param nstr: the string from which a name should be extracted
    :return:
    """
    mo = re.search(r"(^|\s)[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]+"
                   + "((\s|-)[A-ZÀÂÄÈÉÊËÏÔŒÙÛÜŸ][a-zàáâäéèêëíìîïòóôöúùûüøœæç]+)*($|\s|,)", nstr)
    if mo is not None:
        return mo[0]
    else:
        return None


def namebuild(nstr):
    """
    try to
    - match an abbreviated first name from a name string
    - extract a full first name from an abbreviated version using conversion tables
    :param nstr: the string from which to extract a name
    :return: firstnm, matchstr, rebuilt, abv
    - firstnm :
      - if a name is matched, the full form if possible; else, the full form where
        possible and an nothing elsewhere
      - an empty string if no name is matched
    - matchstr : the string that has been matched as a name, to modify the name variable from
      prep_query()
    - rebuilt : a boolean value indicating wether the name has been rebuilt (and can be trusted in queries)
    - abv : a boolean indicating if the name contains abbreviations:
      - True if the firstnm contains abbreviations
      - False if not
      - None if no name string has been matched
    """
    firstnm = ""  # full first name to be extracted
    matchstr = ""  # the matched string
    rebuilt = False  # boolean indicating wether the first name is rebuild (can be trusted)
    #                  or not
    # abv = None  # boolean indicating wether the name contains an abbreviation or not:
    #             we try to rebuild a full name from abbreviation, but can't always; in
    #             that case, abv is True to indicate that the name contains an abv

    # CASE 1 - if it is a composed abbreviated first name, try to build a full version
    if rgx_abvcomp(nstr) is not None:
        abvcomp = rgx_abvcomp(nstr)  # the abbreviated composed name
        matchstr = abvcomp
        # clean the composed name
        abvcomp = re.sub(r"(^\s|\s$|\.)", "", abvcomp)
        abvcomp = re.sub(r"-", " ", abvcomp).lower()

        # try to get the complete form from the composed name dictionary
        for k, v in comp_names.items():
            if abvcomp == k:
                firstnm = v  # replace add the complete version of the matched name to firstnm
                rebuilt = True
                # abv = False  # boolean indicating wether firstnm contains abbreviations

        # if no composed name is returned, try to rebuild a composed name
        # - first, try to rebuild the composed name from a smaller set of letters
        #   in the comp_names dictionary (J.-P.-Guillaume => "J.-P." is in the dictionary)
        # - then, try to rebuild the name from non-composed names in the the names dictionary
        # example: J.-P.-Ch. => jean pierre charles
        if firstnm == "":
            ndict = {n: False for n in abvcomp.split()}  # dictionary of subnames: "N." in "P.-N."; the
            #                                              boolean indicates wether a match has been found
            # get a full name from composed name: here, J.-P. would be matched
            for k, v in comp_names.items():
                if k in abvcomp:
                    firstnm += f"{v} "  # add the full matched name
                    k = k.split()  # split the matching composed name in a list to mark the terms
                    #                list items as matched in ndict
                    # abv = False
                    for i in k:
                        ndict[i] = True  # mark matched subnames as true
                    rebuilt = True
                    break  # stop the loop. it's super unlikely that 2 names in comp_names are in abvcomp with 0 errors
            # get a full name from a single name: here, Ch. would be matched. if there is a match,
            # the full name is added to firstnm ; else, firstnm stays empty: initials don't give results
            # in wikidata
            for name, found in ndict.items():
                if found is False:  # if a full version hasn't been found for an initial (name)
                    for k, v in names.items():
                        if name == k:
                            firstnm += f"{v} "  # add the full version to the name
                            found = True
                            ndict[k] = True  # indicate that the full name has been found
                            rebuilt = True
            # check whether there are still abbreviations in the name to give a value to abv
            # if False in ndict.values():
            #     abv = True
            # else:
            #     abv = False

    # CASE 2 - if it is a "simple" (non-composed) abbreviated name, try to build a full name
    elif rgx_abvsimp(nstr) is not None:
        abvsimp = rgx_abvsimp(nstr)  # the abbreviated non-composed name
        matchstr = abvsimp
        abvsimp = re.sub(r"(^\s|\s$|\.)", "", abvsimp).lower()
        # try to get the complete name from the names dictionary
        for k, v in names.items():
            if abvsimp == k:
                firstnm = v
                rebuilt = True
                # abv = False
        # if abv is None:
        #     abv = True

    # CASE 3 - if a full name is matched : first, check if it's a mismatch (aka, the
    # matched full name is a key in names or comp_names)
    elif rgx_complnm(nstr) is not None:
        complnm = rgx_complnm(nstr)  # try to match a full name
        matchstr = complnm
        mismatch = False
        # try to see if it's actually an abbreviated composed name
        complnm = re.sub(r"-", " ", complnm.lower())
        for k, v in comp_names.items():
            if complnm == k:
                firstnm = v
                mismatch = True
        for k, v in names.items():
            if complnm == k:
                firstnm = v
                mismatch = True
        if mismatch is False:
            firstnm = complnm

    firstnm = firstnm.replace(",", "")

    return firstnm, matchstr, rebuilt
