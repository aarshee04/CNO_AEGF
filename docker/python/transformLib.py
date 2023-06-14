
def transform(val, column):
    retVal = str(val)

    if "transformations" in column:
        for trans in column["transformations"]:
            if trans["function"] == "mask":
                retVal = mask(retVal, trans)
            elif trans["function"] == "trim":
                retVal = trim(retVal, trans)
            elif trans["function"] == "ssn_lookup":
                retVal = ssnLookup(retVal, trans)
            elif trans["function"] == "lob_lookup":
                retVal = lobLookup(retVal, trans)

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

def ssnLookup(val, transform):
    retVal = val
    
    return retVal

def lobLookup(val, transform):
    retVal = val
    
    return retVal
