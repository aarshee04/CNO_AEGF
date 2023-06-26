"""
* scripts - mqa_common.py
* Library Script to Manage related to General MQAttach Product Tasks
* This script should be imported and both the init and destroy called if used as this will initialize mongo collections
* and destroy connections
*
* @author: MQAttach - Mac Bhyat
* Copyright 2008 MQAttach Canada Inc and all associated companies - All rights reserved
* Date: 14 June 2023
"""

import pymqi
import signal
from bson import ObjectId
from datetime import datetime, timedelta
from colorama import Fore

company_id = 2  # company id
g_continue_listening = True


def is_string_empty(s):
    return s is None or s == ""


def init(mongo_client, mongo_db):
    global current_connection
    current_connection = None
    current_connection = mongo_client

    global current_db
    current_db = mongo_db

    global progress_collection
    progress_collection = current_db["tmp_inprogress"]

    global list_collection
    list_collection = current_db["prm_list"]


def get_mongo_db():
    return current_db


def destroy():
    # routine to cleanup connections and close
    if (current_connection is not None):
        current_connection.close()


def read_progress(progress_id):
    # routine to read the progress record and return
    find_result = None
    must_update = False

    try:
        find_result = progress_collection.find_one({"_id": ObjectId(progress_id)})
        must_update = True
    except NameError as err:
        must_update = False
    
    return find_result, must_update


def update_progress(record):
    # routine to update the given record back to mongo
    if record is None:
        return record
    
    progress_collection.update_one({"_id": record["_id"]}, {"$set":
        {
            "description": record["description"],
            "info": record["info"],
            "title": record["title"],
            "status": record["status"],
            "progress": record["progress"],
            "expire_at": record["expire_at"],
            "start_time": record["start_time"]
        }})
    
    return record


def commit_progress(record):
    # routine to update the record prior to commit
    if record["start_time"] is None:
        record["start_time"] = datetime.now()

    if record["expire_at"] is None:
        record["expire_at"] = record["start_time"] + timedelta(days=5)

    update_progress(record)


def append_cli_step(record, step: str):
    # routine to update the cli step
    if not is_string_empty(step):
        if record["info"]["lines"] is None:
            record["info"]["lines"] = []

        record["info"]["lines"].append(step)

    return record


def step_cli(record, description: str, step: str, progress: int):
    # routine to update the given record with the step and progress and result
    if record is None:
        return

    if progress > 0:
        record["progress"] = progress

    if not is_string_empty(description):
        record["description"] = description

    record["status"] = 2
    record = append_cli_step(record, step)
    commit_progress(record)

    return record


def complete_cli(record):
    # routine to update the given progress record with complete
    record["status"] = 0
    record["description"] = "Instruction Completed Successfully"
    record["progress"] = 100
    commit_progress(record)


def fail_cli(record, exc: Exception):
    # routine to fail the cli
    record["progress"] = 100
    record["status"] = 999
    record["description"] = "Request Failed - Check the Log."

    if record["info"]["lines"] is None:
        record["info"]["lines"] = []
    
    record["info"]["lines"].append("Error Encountered\n" + str(exc))
    record["info"]["error"] = str(exc)


def find_customer_list_for_type(list_type: str):
    # routine to find customer lists for the given type and return the collection and the cursor
    return find_customer_list({}, list_type)


def get_list_collection():
    return list_collection


def find_customer_list(conditions: dict, list_type: str):
    conditions["company_id"] = company_id
    conditions["type"] = list_type.upper()
    return find_customer_list_for_dict(conditions)


def find_customer_list_for_dict(conditions):
    results = list_collection.find(conditions)
    return list_collection, results


def create_customer_list_record(code: str, description: str, type: str):
    # routine to create a list record and return it
    obj = {}
    obj["_id"] = None
    obj["code"] = code.upper()
    obj["company_id"] = company_id
    obj["type"] = type.upper()
    obj["data"] = {}

    if (description is None or len(description) == 0):
        obj["description"] = code.upper()
    else:
        obj["description"] = description.strip()

    obj["schema_version"] = 1
    obj["sys_date"] = datetime.now()

    return obj


def wmq_SignalHandler_SIGINT(SignalNumber, Frame):
    # routine to stop the listener when a ctrl-c is pressed
    print("Interrupt signal received, terminating MQ Listener...")
    global g_continue_listening
    g_continue_listening = False


def wmq_connect(connect_dict, logger):
    # routine to connect a queue manager and queue based on the options provided
    last_exc = None
    max_retry = int(connect_dict["conn_retries"]) if connect_dict["conn_retries"] is not None else 10
    attempts = 0
    qmgr = None
    queue = None

    while attempts < max_retry:
        attempts += 1

        try:
            if (connect_dict["host"] is None):
                # server bindings
                qmgr = pymqi.connect(connect_dict["qm"])
            else:
                # client bindings
                host_port = '%s(%s)' % (connect_dict["host"], connect_dict["port"])
                qmgr = pymqi.connect(connect_dict["qm"], connect_dict["channel"], host_port)

            # get the queue
            queue = pymqi.Queue(qmgr, connect_dict["queue"], connect_dict["open_options"])
            break
        except pymqi.MQMIError as mqe:
            last_exc = mqe

            if attempts >= max_retry:
                #logger.error(f"Unable to connect to QM even after {attempts} attempts", mqe)
                raise Exception(f"Unable to connect to qmgr even after {attempts} attempts: {str(mqe)}")
            
            attempt_reconnect = wmq_must_reconnect(mqe)
            
            if not attempt_reconnect:
                break
        except Exception as ex:
            # Handle other exceptions
            last_exc = ex

            if attempts >= max_retry:
                #logger.error("Unable to connect to QM Attempt {attempts}".format(attempts=attempts), ex)
                raise Exception(f"Unable to connect to qmgr even after {attempts} attempts: {str(ex)}")

    if (queue is not None):
        logger.info(f"Successfully connected to queue manager {connect_dict['qm']}")
    else:
        raise Exception("Terminating QM Connection, {count} attempts exceeded".format(count=max_retry), last_exc)

    return qmgr, queue


def wmq_disconnect(queue, qmgr):
    # routine to disconnect a queue and queue manager
    if (queue is not None):
        queue.close()
    if (qmgr is not None):
        qmgr.disconnect()

def wmq_must_reconnect(mqe: pymqi.MQMIError):
    # routine to return if the queue manager needs to be reconnected
    return (mqe.reason == pymqi.CMQC.MQRC_Q_MGR_NOT_AVAILABLE or mqe.reason == pymqi.CMQC.MQRC_Q_MGR_QUIESCING or
            mqe.reason == pymqi.CMQC.MQRC_Q_MGR_STOPPING or mqe.reason == pymqi.CMQC.MQRC_HOBJ_ERROR or
            mqe.reason == pymqi.CMQC.MQRC_HANDLE_NOT_AVAILABLE or mqe.reason == pymqi.CMQC.MQRC_CHANNEL_STOPPED or
            mqe.reason == pymqi.CMQC.MQRC_CONNECTION_STOPPED or mqe.reason == pymqi.CMQC.MQRC_CONNECTION_STOPPING or
            mqe.reason == pymqi.CMQC.MQRC_HOST_NOT_AVAILABLE or mqe.reason == pymqi.CMQC.MQRC_CHANNEL_NOT_AVAILABLE or
            mqe.reason == pymqi.CMQC.MQRC_CONNECTION_BROKEN or mqe.reason == pymqi.CMQC.MQRC_CONNECTION_ERROR or
            mqe.reason == pymqi.CMQC.MQRC_HCONN_ERROR)


def wmq_message_listener(logger, connection_dict, callback):
    qmgr = None
    queue = None

    try:
        # initialize
        qmgr, queue = wmq_connect(connection_dict, logger)

        logger.info("Initializing Message Listener...")

        # attach the signal handler
        signal.signal(signal.SIGINT, wmq_SignalHandler_SIGINT)

        # get the wait interval
        interval = 5
        if connection_dict.get("wait_interval") is not None:
            interval = int(connection_dict["wait_interval"])

        # Get Message Options
        gmo = pymqi.GMO()
        gmo.Options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING | pymqi.CMQC.MQGMO_CONVERT
        gmo.WaitInterval = interval * 1000  # 5 seconds

        while g_continue_listening:
            try:
                # get the message from the queue
                message = queue.get(None, None, gmo)
                # process the message
                wmq_process_message(logger, message, callback)
            except pymqi.MQMIError as mqe:
                if mqe.comp == pymqi.CMQC.MQCC_FAILED and mqe.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    # no message available - continue listening
                    continue
                elif wmq_must_reconnect(mqe):
                    qmgr, queue = wmq_connect(connection_dict)

                    if queue is None:
                        break

                    continue
                else:
                    raise Exception(f"MQ error while processing message {str(mqe)}")
            except Exception as ex:
                raise Exception(f"Other exception while processing message {str(ex)}")
    except pymqi.MQMIError as mqe:
        logger.error(f"{Fore.RED}Terminating QM Connection, attempts exceeded {Fore.BLACK}", mqe)
    except Exception as e:
        logger.error(f"{Fore.RED}{str(e)}{Fore.BLACK}")
    finally:
        # cleanup
        wmq_disconnect(queue, qmgr)


def wmq_process_message(logger, message, callback):
    # routine to process the given message via the given callback
    try:
        callback(message)
    except Exception as ex:
        logger.error(f"{Fore.RED}Exception while processing message {str(ex)} {Fore.BLACK}")
