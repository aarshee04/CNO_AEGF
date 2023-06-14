import pandas as pd
import json
import commonLib as comlib
import transformLib as translib
from datetime import datetime
import sys
import configparser

props = configparser.ConfigParser()
props.read("crs.properties")

baseUrl = props.get("API", "BASE_URL")

curTime = str(datetime.now().strftime("%Y-%m-%dT%H.%M.%S"))
outFile = f"/home/gubbi/python/PolicyReport_{curTime}.csv"

id = sys.argv[1] if len(sys.argv) >= 2 else ""

# connect to database
conn = comlib.connectToSqlSvr()

# retrieve the rows as dictionary
cursor = conn.cursor(as_dict=True)
cursor.callproc('dbo.PRC_GET_AEGF_POLICIES', (285856, '1,2,3', '2012-01-01 00:00:00', '2023-08-01 00:00:00'))

# loop thru the rows
#for row in cursor:
    #print(row)

# get all rows
rows = cursor.fetchall()

# convert the rows dictionary as pandas dataframe
df = pd.DataFrame(rows)

# call config API
url = f"{baseUrl}/{id}"
data = comlib.getApiCall(url)

# build ouptut dictionary by applying transformations
if data:
    result = {}

    # loop thru the response object
    for column in data["columns"]:
        delim = ""
        srcList = []

        for src in column["data_source"]:
            if src[0] == "$":
                srcList.append(src[1:-1])
            else:
                delim = src if delim == src else (delim + src)

        if len(srcList) == 1:
            result[column["caption"]] = list(df[srcList[0]].apply(lambda val: translib.transform(val, column)))
        else:
            result[column["caption"]] = list(df[srcList].apply(lambda val: translib.transform(delim.join(val), column), axis = 1))

    # convert dictionary into json
    jsonObj = json.dumps(result, indent = 4)
    print(jsonObj)

    # convert the dictionary as dataframe and in turn write as csv
    # sep -> custom delimiter
    # index -> to include/exclude index column, index_label -> custom name for index column
    # quoting -> to enclose the values using quote char, quotechar -> custom quote char
    csvParms = comlib.parseOutOptions(data["output"])

    #pd.DataFrame(result).to_csv(outFile, sep="|", index=False, index_label="Row No.", quoting=csv.QUOTE_NONNUMERIC, quotechar="'")
    pd.DataFrame(result).to_csv(outFile, **csvParms)
else:
    print("No data to process")