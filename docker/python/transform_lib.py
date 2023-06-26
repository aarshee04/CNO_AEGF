
def transform(val, column, ssn_xref, lob_xref):
    retVal = str(val)

    if "transformations" in column:
        for trans in column["transformations"]:
            if trans["function"] == "mask":
                retVal = mask(retVal, trans)
            elif trans["function"] == "trim":
                retVal = trim(retVal, trans)
            elif trans["function"] == "ssn_lookup":
                retVal = ssnLookup(retVal, trans, ssn_xref)
            elif trans["function"] == "lob_lookup":
                retVal = lobLookup(retVal, trans, lob_xref)
            elif trans["function"] == "deduction_lookup":
                retVal = deductionLookup(retVal, trans, lob_xref)

    return retVal

def mask(val, transform):
    # convert str to list of chars as str object is immutable
    retVal = list(val)

    maskPos = transform["parameters"]["chars"]
    maskChar = transform["parameters"]["char"]

    for pos in maskPos.split(","):
        # pos - 1 points to the actual char in the list to be masked
        charPos = int(pos) - 1
        retVal[charPos] = maskChar

    return "".join(retVal)

def trim(val, transform):
    retVal = val

    if "all" in transform["parameters"]:
        retVal = str(val).strip().replace("  ", " ")
    elif "right" in transform["parameters"]:
        retVal = str(val).rstrip()
    elif "left" in transform["parameters"]:
        retVal = str(val).lstrip()
    
    return retVal

def ssnLookup(key, transform, ssn_xref):
    retVal = key
    
    if(key in ssn_xref):
        retVal = ssn_xref[key]

    return retVal

def lobLookup(key, transform, lob_xref):
    retVal = key
    
    if(key in lob_xref):
        retVal = lob_xref[key]["code"]

    return retVal

def deductionLookup(key, transform, lob_xref):
    retVal = key
    
    if(key in lob_xref):
        retVal = lob_xref[key]["deduction"]

    return retVal
