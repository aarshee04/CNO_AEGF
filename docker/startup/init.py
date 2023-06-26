from crontab import CronTab
import subprocess
import os

scriptDir = os.path.dirname(os.path.abspath(__file__))
scriptDir = scriptDir.replace("startup", "python")

cron = CronTab(user = True)
cronId = "health-check"
matchingJobs = list(cron.find_comment(cronId))

# if job does not exist
if len(matchingJobs) == 0:
    # health check program runs every minute to check if the scheduler is running
    job = cron.new(command=f"cd {scriptDir} ; python3 {scriptDir}/health_check.py", comment=cronId)
    job.minute.every(1)
    cron.write()

# run the health check program which start the scheduler if one is not running
subprocess.Popen(["python3", f"{scriptDir}/health_check.py"])