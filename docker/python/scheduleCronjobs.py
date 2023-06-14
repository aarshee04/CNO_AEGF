from crontab import CronTab, CronItem
import commonLib as comlib
import cronLib as cronlib
import configparser
import pymqi
import json

# reading message from Queue
props = configparser.ConfigParser()
props.read("crs.properties")

q = props.get("IBM_MQ", "QUEUE")

data=""

try:
    qmgr = comlib.getQmConnection()
    queue = pymqi.Queue(qmgr, q)

    jsonStr = queue.get()
    data = json.loads(jsonStr)

    queue.close()
except pymqi.MQMIError as err:
    if err.comp == pymqi.CMQC.MQCC_FAILED and err.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
        print("No message available to process")
        queue.close()
        pass
    else:
        print(f"MQ operation failed: {err}")

if qmgr.is_connected:
    qmgr.disconnect()

if data:
    # scheduling new job
    cront = CronTab(user = True)

    id = data["_id"]
    # delete the matching job if exists
    cronlib.delCronJob(cront, id)

    command = f"cd /var/scripts/python; /usr/bin/python3 /var/scripts/python/generateReport.py {id} # {id}"

    for exec in data["schedule"]["executions"]:
        expr = "*/15 * * * *" #exec["cron"]
        job = CronItem.from_line(expr + ' ' + command, cron=cront)
        cront.append(job)

    cront.write()

    # list jobs after adding/updating
    cronlib.listCronjobs(cront)
