import pymongo as mongo
import configparser
import os

scriptDir = os.path.dirname(os.path.abspath(__file__))

props = configparser.ConfigParser()
props.read(f"{scriptDir}/crs.properties")

section = "MONGO"
mhost = props.get(section, "HOST")
mport = int(props.get(section, "PORT"))
mdb = props.get(section, "DB")

mcli = mongo.MongoClient(mhost, mport)
db = mcli[mdb] # set the database name
prmList = db["prm_list"]

srch = {}
srch["type"] = "AEGF_EMP_CODE"
srch["data.group"] = 11310

selEle = {}
selEle["data.group"] = 1
selEle["data.emp_code"] = 1
selEle["data.ssn"] = 1

recs = prmList.find(srch, selEle)

for rec in recs:
    print(rec)
