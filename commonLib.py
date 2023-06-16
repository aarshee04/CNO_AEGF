import pymssql
import configparser
import csv
import requests as rest
import pymqi
import os

scriptDir = os.path.dirname(os.path.abspath(__file__))

def connectToSqlSvr():
    # reading properties files
    props = configparser.ConfigParser()
    props.read(f"{scriptDir}/crs.properties")

    sqlSection = "SQLSVR"
    dbHost = props.get(sqlSection, "HOST")
    dbUsr = props.get(sqlSection, "USER")
    dbPwd = props.get(sqlSection, "PWD")
    dbName = props.get(sqlSection, "DB")

    dbconn = pymssql.connect(server=dbHost, user=dbUsr, password=dbPwd, database=dbName)

    return dbconn

def getQmConnection():
    # reading properties files
    props = configparser.ConfigParser()
    props.read(f"{scriptDir}/crs.properties")

    mqSection = "IBM_MQ"
    qm = props.get(mqSection, "QM")
    channel = props.get(mqSection, "CHANNEL")
    host = props.get(mqSection, "HOST")
    port = props.get(mqSection, "PORT")
    connInfo = '%s(%s)' % (host, port)
    encoding = props.get(mqSection, "ENCODING")
    ccsid = props.get(mqSection, "CCSID")

    qmgr = pymqi.connect(qm, channel, connInfo, bytes_encoding = encoding, default_ccsid = ccsid)

    return qmgr

def getApiCall(url):
    resp = rest.get(url)
    return resp.json() if (resp.status_code >= 200 and resp.status_code < 300) else ""

def parseOutOptions(outputFormat):
    params = {
        "index": False,
        "index_label": "Row No." # this applies when index is True
    }

    outOptions = outputFormat["options"]

    if "header" in outOptions:
        params["header"] = True if outOptions["header"] else False

    if "delimiter" in outOptions:
        params["sep"] = outOptions["delimiter"]

    if "text_delimiter" in outOptions:
        params["quoting"] = csv.QUOTE_NONNUMERIC
        params["quotechar"] = outOptions["text_delimiter"]

    return params
