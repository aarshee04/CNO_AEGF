from crontab import CronTab
import os
import commonLib as comlib
import cronLib as cronlib

# call config API
url = "https://web01.dc.mqattach.com/NETCORE/api/client/cno/aegf/execute/647999b8b8aa3316b4de4377"
data = comlib.getApiCall(url)

cron = CronTab(user = True)

id = data["_id"]
# delete the matching job if exists
cronlib.delCronJob(cron, id)

# reading the crontab for the pre-existing jobs
cronPs = os.popen("crontab -l > crontab.txt")
cronPs.read()
cronPs.close()

cronfile = open("crontab.txt", "a")

for exec in data["schedule"]["executions"]:
    expr = "*/5 * * * *" #exec["cron"]
    cronfile.write(f"{expr} /usr/bin/python3 /home/gubbi/python/test.py # {id}\n")

cronfile.close()

cronUpdatePs = os.popen("crontab < crontab.txt")
cronUpdatePs.close()

# list jobs after update
cronlib.listCronjobs(cron)
