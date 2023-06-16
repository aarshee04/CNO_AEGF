from crontab import CronTab, CronItem
import commonLib as comlib
import cronLib as cronlib
import configparser
import pymqi
import json
import signal
import os

scriptDir = os.path.dirname(os.path.abspath(__file__))

def schedule(data):
    # scheduling new job
    cront = CronTab(user = True)

    id = data["_id"]
    # delete the matching job if exists
    cronlib.delCronJob(cront, id)

    command = f"cd {scriptDir}; python3 {scriptDir}/generateReport.py {id} # {id}"

    for exec in data["schedule"]["executions"]:
        expr = "*/15 * * * *" #exec["cron"]
        job = CronItem.from_line(expr + ' ' + command, cron=cront)
        cront.append(job)

    cront.write()

    # list jobs after adding/updating
    cronlib.listCronjobs(cront)

def signalHandler(sig, frame):
    global keepRunning
    keepRunning = False

# main program starts from here
sigList = [signal.SIGINT, signal.SIGHUP, signal.SIGKILL, signal.SIGQUIT, signal.SIGABRT, signal.SIGSTOP, signal.SIGTERM]

for sig in sigList:
    signal.signal(signal.SIGINT, signalHandler)

props = configparser.ConfigParser()
props.read(f"{scriptDir}/crs.properties")

mqSection = "IBM_MQ"
q = props.get(mqSection, "QUEUE")
wait = props.get(mqSection, "GET_WAIT")

msg = ""

# Message Descriptor
md = pymqi.MD()

# Get Message Options
gmo = pymqi.GMO()
gmo.Options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
gmo.WaitInterval = int(wait)

keepRunning = True

try:
    qmgr = comlib.getQmConnection()
    queue = pymqi.Queue(qmgr, q)

    while keepRunning:
        try:
            jsonStr = queue.get(None, md, gmo)
            msg = json.loads(jsonStr)

            if msg:
                schedule(msg)
            
            # Reset the MsgId, CorrelId & GroupId so that we can reuse
            md.MsgId = pymqi.CMQC.MQMI_NONE
            md.CorrelId = pymqi.CMQC.MQCI_NONE
            md.GroupId = pymqi.CMQC.MQGI_NONE
        except pymqi.MQMIError as err:
            if err.comp == pymqi.CMQC.MQCC_FAILED and err.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                print("No message available to process")
                pass
            else:
                print(f"MQ GET failed: {err}")
except pymqi.MQMIError as err:
    print(f"Error connecting to Queue Manager: {err}")

# if error connecting to QM, no point in disconnecting
# if qmgr and qmgr.is_connected:
#     queue.close()
#     qmgr.disconnect()