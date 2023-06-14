from croniter import croniter as ci
from datetime import datetime as dt

# crontab file location - /var/spool/cron/crontab/<user>

def listCronjobs(cron):
    print("List of cron jobs:")
    print("------------------")

    for job in cron:
        # to access the comment id of the cronjob
        #print(job.comment)
        print(job)

def findCronjobById(cron, id):
    matchingJobs = list(cron.find_comment(id))
    return matchingJobs[0] if len(matchingJobs) > 0 else ""

def scheduleCronjob(cron, id="CRS_Billing"):
    job = findCronjobById(cron, id)

    if job:
        print(f"Job with the id '{id}' exists, replacing the job")
        cron.remove(job)
        cron.write()

    job = cron.new(command="/usr/bin/python3 /home/gubbi/python/test.py", comment=id)
    job.minute.every(1)
    cron.write()

def delAllCronjobs(cron):
    # cron.remove(job) # to delete a specific job
    cron.remove_all()
    cron.write() # this is mandatory to persist the changes

    print("All cron jobs deleted")

def delCronJob(cron, id):
    job = findCronjobById(cron, id)

    if job:
        cron.remove(job)
        cron.write()
        print(f"Cron job with matching id deleted")
    else:
        print("No matching job found")

def nextRunOfCronjob(cron, id):
    job = findCronjobById(cron, id)

    if job:
        jobSpec = " ".join(list(str(job).split(" "))[:5])
        iter = ci(jobSpec, dt.now())
        print(f"Job's next run is scheduled at {iter.get_next(dt)}")
    else:
        print("No matching job found")    
