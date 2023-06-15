from crontab import CronTab
import subprocess
import os

scriptDir = os.path.dirname(os.path.abspath(__file__))
scriptDir = scriptDir.replace("startup", "python")

cron = CronTab(user = True)

# health check program runs every minute to check if the scheduler is running
job = cron.new(command=f"cd {scriptDir} ; python3 {scriptDir}/healthCheck.py", comment="health-check")
job.minute.every(1)
cron.write()

# run the health check program which start the scheduler if one is not running
subprocess.Popen(["python3", f"{scriptDir}/healthCheck.py"])