"""
* scripts - common.py
* Common Library used by the Automated Employer Group File Backend
* This script will be imported by others
*
* @author: MQAttach - Ravi
* Copyright 2008 MQAttach Canada Inc and all associated companies - All rights reserved
* Date: 14 June 2023
"""

import pymssql
import configparser
import requests as rest
import pymongo
import logging
from bson import ObjectId
from collections import namedtuple
import sys
import os

import mqa_common as mqalib

scriptDir = os.path.dirname(os.path.abspath(__file__))

list_types = {"group": "AEGF_GROUP", "ssn": "AEGF_EMP_CODE", "lob": "AEGF_LOB"}
definition_collection_name = "mst_aegf_billing"

def getLogger():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger()
    
def readProperties():
    # routine to read the common property file
    props = configparser.ConfigParser()
    props.read(f"{scriptDir}/config.properties")
    return props

def connectToSqlSvr():
    # routine to connect to SQL
    props = readProperties()

    sqlSection = "SQLSVR"
    dbHost = props.get(sqlSection, "HOST")
    dbUsr = props.get(sqlSection, "USER")
    dbPwd = props.get(sqlSection, "PWD")
    dbName = props.get(sqlSection, "DB")

    dbconn = pymssql.connect(server=dbHost, user=dbUsr, password=dbPwd, database=dbName)

    return dbconn

def connectMongo():
    # routine to connect to mongo using pymongo and return connection and the database
    props = readProperties()
    section = "MONGO"
    dbclient = pymongo.MongoClient(props.get(section, "URI"))
    dbconn = dbclient[props.get(section, "DB")]

    '''
    dbnames = dbclient.list_database_names()

    if props.get(section, "DB") not in dbnames:
        raise Exception("Mongo DB not found")
    '''
    return dbclient, dbconn


def getQmConnectionInfo():
    # routine to return the the connection information for the queue manager as a dictionary
    props = readProperties()

    mqSection = "IBM_MQ"
    connect_dict = {}
    connect_dict["qm"] = props.get(mqSection, "QM")
    connect_dict["queue"] = props.get(mqSection, "QUEUE")
    connect_dict["channel"] = props.get(mqSection, "CHANNEL")
    connect_dict["port"] = props.get(mqSection, "port")
    connect_dict["host"] = props.get(mqSection, "HOST")
    connect_dict["wait_interval"] = props.get(mqSection, "GET_WAIT")
    connect_dict["conn_retries"] = props.get(mqSection, "CONN_RETRY")

    return connect_dict


def get_output_parameters():
    # routine to return the output information
    # playing around with named tuples
    props = readProperties()

    section = "OUTPUT"
    return_dict = {"prefix": props.get(section, "PREFIX"),
                   "temp_path": props.get(section, "TEMP"),
                   "delivery_path": props.get(section, "DELIVERY")}
    output_obj = namedtuple("output", return_dict.keys())
    return_obj = output_obj(**return_dict)
    return return_obj


def get_definition(report_id: str):
    # routine to return a definition for the given report id
    # the reason we do a rest call here as opposed to reading directly to the purpose of holiday determination
    props = readProperties()
    url = f"{props.get('API', 'BASE_URL')}/{report_id}"
    resp = rest.get(url)
    return resp.json() if (resp.status_code >= 200 and resp.status_code < 300) else ""


def update_execution(report_id: str, data: dict):
    # routine to update the execution details on the report
    conn = mqalib.get_mongo_db()
    record_id = ObjectId(report_id)
    definition_collection = conn[definition_collection_name]
    record = definition_collection.find_one({"_id": record_id})
    if record is None:
        return None
    result = definition_collection.update_one({'_id': record_id}, {"$currentDate": {"sys_date": True}, "$set": {
        'last_execution': data,
    }})
    return result

def update_nextrun(report_id: str, next_schedule: str):
    conn = mqalib.get_mongo_db()
    record_id = ObjectId(report_id)
    mongo_coll = conn[definition_collection_name]
    record = mongo_coll.find_one({"_id": record_id})

    if record is not None:
        result = mongo_coll.update_one({"_id": record_id}, {"$set": {"next_run": next_schedule}})
    
    return result

def get_master_groups(populate_sub_groups: bool):
    # routine to return an array of all master group codes
    collection, cursor = mqalib.find_customer_list_for_type(list_types["group"])
    master_group_array = []
    for row in cursor:
        # work out the sub-groups for this master group
        sub_groups = None
        if populate_sub_groups and row["data"].get("sub_groups") is not None:
            sub_groups = []
            for item in row["data"]["sub_groups"]:
                sub_groups.append(int(item["code"]))

        # append to the array
        master_group_array.append({"group": row["code"], "sub_groups": sub_groups})
    return master_group_array


def get_master_subgroups(employee_group_id):
    # routine to return the sub_group list for a given employee group id

    list_collection = mqalib.get_list_collection()
    cursor = list_collection.find({"code": str(employee_group_id), "type": list_types["group"], "company_id": 2})

    # TODO: Figure out why this does not work in python
    # conditions = {"code": str(employee_group_id)}
    # list_collection, cursor = mqalib.find_customer_list(conditions, list_types["group"])

    first_item = next(cursor, None)

    return_array = []
    if (first_item is not None):
        for item in first_item["data"]["sub_groups"]:
            return_array.append(int(item["code"]))
    return return_array


def get_list_types():
    return list_types


def process_list_update(mongo_collection, record):
    # routine to perform the write of the given dictionary we could use upsert here
    # but it would make the update more complicate as we would need to set everything
    is_insert = record["insert"]
    del record["insert"]
    if is_insert:
        result = mongo_collection.insert_one(record)
    else:
        result = mongo_collection.update_one({'_id': record["_id"]}, {"$currentDate": {"sys_date": True}, "$set": {
            'data': record["data"],
            'description': record["description"],
        }})
    return result


def process_master_sync(logger, title, args, filter_callback, sync_callback):
    print("{title} Sync Started...".format(title=title))

    # initialize variables
    sql_client = None
    update_progress = False
    try:
        # initialize
        logger.info("Initializing Connection to Mongo...")
        mongo_client, mongo_db = connectMongo()
        mqalib.init(mongo_client, mongo_db)

        # check for a progress record
        if args.progress_id is not None:
            print("CLI Invocation with Progress Id {id}...".format(id=args.progress_id[0]))
            logger.info("CLI Invocation - Reading Progress Id {id}".format(id=args.progress_id[0]))
            progress_record, update_progress = mqalib.read_progress(args.progress_id[0])

        master_group_array = filter_callback(args)

        if update_progress:
            mqalib.step_cli(progress_record, "Connecting to SQL...", None, 20)

        logger.info("Connecting to SQL....")
        sql_client = connectToSqlSvr()

        # initialize counter variables
        count = 0
        errors = 0
        perc = 0
        total = len(master_group_array)

        # loop through the list
        for group_obj in master_group_array:
            count += 1
            master_group = int(group_obj["group"])
            try:
                sub_groups = group_obj["sub_groups"]
                desc = "Processing Master Group {master_group}...".format(master_group=str(master_group))
                print(desc)
                perc = 10 + (60 * count / total)
                if update_progress:
                    mqalib.step_cli(progress_record, "Processing...", desc, perc)
                    sync_callback(master_group, sub_groups, sql_client)
            except Exception as e:
                errors += 1
                print("There was an error Processing Master Group {master_group}...".format(
                    master_group=str(master_group)))
                if update_progress:
                    mqalib.step_cli(progress_record, "Processing...", str(e), perc)

        if update_progress:
            mqalib.append_cli_step(progress_record, "Update Complete")
        if errors > 0:
            mqalib.fail_cli(progress_record, ValueError("There were errors encountered - Check the Log"))
        else:
            mqalib.complete_cli(progress_record)
        logger.info("Update Complete. Errors: {count}".format(count=errors))
        if errors > 0:
            print("Errors Encountered: {count}...".format(count=errors), file=sys.stderr)
        else:
            print("Update Complete. No Errors")
    except Exception as e:
        if update_progress:
            mqalib.fail_cli(progress_record, e)
            print("Error Processing: {e}...".format(e=str(e)), file=sys.stderr)
    finally:
        # close the connections
        mqalib.destroy()
        if sql_client is not None:
            sql_client.close()
