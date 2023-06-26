"""
* scripts - cron_update.py
* Script to update the cron jobs
* This script will be invoked by message listener process
*
* @author: MQAttach - Ravi
* Copyright 2008 MQAttach Canada Inc and all associated companies - All rights reserved
* Date: 21 June 2023
"""

import argparse
from crontab import CronTab, CronItem
import os
from colorama import Fore

import cron_lib as cronlib
import common_lib as comlib

# declare the logger
logger = comlib.getLogger()

scriptDir = os.path.dirname(os.path.abspath(__file__))

def schedule(id, cronExpr):
    logger.info(f"Updating cron job with id: {id} ...")

    # scheduling new job
    #cront = CronTab(user = True)
    cront = cronlib.getCron()

    # delete the matching job if exists
    cronlib.delCronJob(cront, id, logger)

    # scheduling new job
    command = f"cd {scriptDir}; python3 {scriptDir}/generate_report.py --id={id} # {id}"
    job = CronItem.from_line(cronExpr + ' ' + command, cron=cront)
    cront.append(job)
    cront.write()
    logger.info(f"Job schedule updated successfully ...")

    # getting the next run of the job
    nextRun = cronlib.nextRunOfCronjob(cront, id, logger)

    if nextRun is not None:
        logger.info(f"Job's next run is scheduled at {nextRun}")

def main():
    # main routine that actually does the running as a standalone

    try:
        # parse the command line arguments
        parser = argparse.ArgumentParser(description="Process to schedule and update the cronjobs")
        parser.add_argument("--id", nargs=1, help="Report Definition Identifier", required=True)
        parser.add_argument("--cron", nargs=1, help="cron job expression", required=True)
        parser.add_argument("--progress_id", nargs=1, help="Optional GTM CLI Progress Id", required=False)
        
        args = parser.parse_args()

        schedule(args.id[0], args.cron[0])
    except Exception as e:
        logger.error(f"{Fore.RED}Error while scheduling a job - {str(e)}{Fore.BLACK}")

if __name__ == '__main__':
    main()
