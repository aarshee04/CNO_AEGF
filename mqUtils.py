import pymqi
import sys
import json
import configparser
import commonLib as comlib
import os

scriptDir = os.path.dirname(os.path.abspath(__file__))

# reading properties files
props = configparser.ConfigParser()
props.read(f"{scriptDir}/crs.properties")

mqSection = "IBM_MQ"
q = props.get(mqSection, "QUEUE")

arg = (lambda: "GET", lambda: sys.argv[1])[len(sys.argv) >= 2]()
argl = arg.split("=")
oper = argl[0].upper()
inFile = (lambda: f"{scriptDir}/sampleSchedule.json", lambda: argl[1])[len(argl) == 2]()

try:
    qmgr = comlib.getQmConnection()
    queue = pymqi.Queue(qmgr, q)

    if oper == "GET":
        jsonStr = queue.get()
        dict = json.loads(jsonStr)
        print(f"Full message ---- \n{dict}")

        print(f"\nIndividual records ----")

        for actor in dict["Actors"]:
            print()
            
            for key in actor.keys():
                print(f"{key}: {actor[key]}")
    else:
        with open(inFile) as file:
            data = file.read()
            file.close()

        queue.put(data)
except pymqi.MQMIError as err:
    if err.comp == pymqi.CMQC.MQCC_FAILED and err.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
        print("No message available to process")
        pass
    else:
        print(f"MQ operation failed: {err}")
    
queue.close()
qmgr.disconnect()