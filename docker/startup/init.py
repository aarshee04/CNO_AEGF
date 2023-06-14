from crontab import CronTab

cron = CronTab(user = True)

job = cron.new(command="cd /var/scripts/python ; /usr/bin/python3 /var/scripts/python/scheduleCronjobs.py", comment="scheduler")
job.minute.every(1)
cron.write()
