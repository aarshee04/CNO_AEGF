from crontab import CronTab
import sys
import cronLib as cronlib

# crontab file location - /var/spool/cron/crontab/<user>
argToken = []

def parseArguments():
    # in tuple and dictionary both expressions will be evaluated because of which sys.argv[1] throws error if arguments < 2
    #op = ("list", sys.argv[1])[len(sys.argv) >= 2]

    global argToken

    # lambda is more efficient than the tuple and dictionary because in lambda only one expression is evaluated
    arg = (lambda: "list", lambda: sys.argv[1])[len(sys.argv) >= 2]()
    argl = arg.split("=")

    argToken.append(argl[0])
    argToken.append((lambda: "", lambda: argl[1])[len(argl) == 2]())

# main program starts here
cron = CronTab(user = True)

parseArguments()

op = argToken[0]
id = argToken[1]

if op == "list":
    cronlib.listCronjobs(cron)
elif op == "find":
    job = cronlib.findCronjobById(cron, id)
    print(job if job else "No matching job found")
elif op == "schedule":
    cronlib.scheduleCronjob(cron, id)
elif op == "delete":
    cronlib.delCronJob(cron, id)
elif op == "deleteall":
    cronlib.delAllCronjobs(cron)
elif op == "nextrun":
    cronlib.nextRunOfCronjob(cron, id)
else:
    print(f"Invalid operation ({op}) specified")