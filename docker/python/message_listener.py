"""
* scripts - message_listener.py
* Main Entrypoint of the Server Backend for the Automated Employer Group Files
* This will start a MQ Listener that will listen for messages on a the MQ Configuration or wait for a System Stop Signal
* When a message is found it will invoke another python script with the message arguments
* The UI will write messages to the queue for this to process
*
* @author: MQAttach - Mac Bhyat
* Copyright 2008 MQAttach Canada Inc and all associated companies - All rights reserved
* Date: 15 June 2023
"""
import logging
import subprocess
import pymqi
import json
from colorama import Fore
import os

import common_lib as comlib
import mqa_common as mqalib

scriptDir = os.path.dirname(os.path.abspath(__file__))

def main():
    # routine to begin listening on the queue
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()

    try:
        # get the connection information
        connect_dict = comlib.getQmConnectionInfo()
        connect_dict["open_options"] = pymqi.CMQC.MQOO_FAIL_IF_QUIESCING | pymqi.CMQC.MQOO_INPUT_SHARED

        mqalib.wmq_message_listener(logger, connect_dict, handle_message)
    except Exception as e:
        logger.error(f"{Fore.RED}Encountered an error while processing MQ message: {str(e)}{Fore.BLACK}")

def handle_message(message):
    # routine to handle the message received from the queue
    json_data = json.loads(message)

    # create the args array
    process_args = []

    # look for a progress_id in the message
    if "ProgressId" in json_data:
        process_args.append(f"--progress_id={json_data['ProgressId']}")

    script_name = None

    # now see if there any arguments that are not the script name
    for key, value in json_data["Arguments"].items():
        if (key == "script"):
            script_name = f"{scriptDir}/{value}"
        else:
            process_args.append(f"--{key}={value}")

    process_args.insert(0, script_name)
    process_args.insert(0, "python3")

    subprocess.run(process_args)

if __name__ == '__main__':
    main()
