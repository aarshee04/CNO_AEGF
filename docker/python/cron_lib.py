from croniter import croniter as ci
from datetime import datetime as dt
from crontab import CronTab

# crontab file location - /var/spool/cron/crontab/<user>

def getCron():
    cront = CronTab(user = True)
    return cront

def listCronjobs(cron):
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

    job = cron.new(command="python3 /home/gubbi/python/test.py", comment=id)
    job.minute.every(1)
    cron.write()

def delAllCronjobs(cron):
    # cron.remove(job) # to delete a specific job
    cron.remove_all()
    cron.write() # this is mandatory to persist the changes

    print("All cron jobs deleted")

def delCronJob(cron, id, logger):
    job = findCronjobById(cron, id)

    if job:
        cron.remove(job)
        cron.write()
        logger.info(f"Cron job with matching id {id} deleted ...")
    else:
        logger.info(f"No matching cron job found for id {id} ...")

def nextRunOfCronjob(cron, id, logger):
    job = findCronjobById(cron, id)

    if job:
        jobSpec = " ".join(list(str(job).split(" "))[:5])
        iter = ci(jobSpec, dt.now())
        #print(f"Job's next run is scheduled at {iter.get_next(dt)}")
        return iter.get_next(dt)
    else:
        logger.info("No matching job found to get the next scheduled run ...")    

    return None