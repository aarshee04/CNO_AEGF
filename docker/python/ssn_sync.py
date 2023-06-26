"""
* scripts - ssn_sync.py
* Script to Manage the Syncing of the Employees for a given Employer Group Code from the CRS Source System
* This script will be invoked by the UI or through an import during the generation process
*
* @author: MQAttach - Mac Bhyat
* Copyright 2008 MQAttach Canada Inc and all associated companies - All rights reserved
* Date: 15 June 2023
"""

import argparse
import logging
from bson import ObjectId

import common_lib as comlib
import mqa_common as mqa_comlib

# declare the dictionary that will hold the data that needs to be looked up against and eventually written to mongo
data_dict = {}
mongo_collection = None
list_type = comlib.get_list_types()["ssn"]

# declare the logger
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


def process_dest_row(row):
    # routine to update the the mongo row into the dictionary
    code = row["data"]["ssn"]
    row["insert"] = False
    data_dict[code] = row


def process_source_row(employee_group_id, row):
    # routine to process the actual source row into the dictionary
    ssn = row["SSN"]
    if ssn is None:
        return

    delete_flag = False if row["DELETEFLAG"] == 0 else True
    if ssn in data_dict:
        obj = data_dict[ssn]
    else:
        if delete_flag:
            return  # if its deleted and we don't have it we don't care
        # create a new record
        code = ObjectId()
        obj = mqa_comlib.create_customer_list_record(str(code), "{first} {last}".format(first=row["FIRSTNAME"],
                                                                                        last=row["LASTNAME"]),
                                                     list_type)
        obj["data"] = {"ssn": ssn, "group": employee_group_id, "status": 1}
        obj["_id"] = code
        obj["insert"] = True
        data_dict[ssn] = obj

    # update the groups
    obj["data"]["group"] = employee_group_id
    if "sub_group" in obj["data"]:
        del obj["data"]["sub_group"]
    if row["GROUPID"] is not None:
        obj["data"]["sub_group"] = row["GROUPID"]

    # update the employee code
    source_code = row["EMPLOYEEID"]
    if source_code is not None:
        dest_code = None if "emp_code" not in obj["data"] else obj["data"]["emp_code"]
        if dest_code is None:
            obj["data"]["emp_code"] = source_code

    # update the name
    name = row["FIRSTNAME"]
    if not mqa_comlib.is_string_empty(row["MIDDLENAME"]):
        name = name + " " + row["MIDDLENAME"]
    name = name + " " + row["LASTNAME"]
    obj["description"] = name


def perform_sync(employer_group_id, employer_sub_group_list, sql_client):
    # routine to perform the sync or throw an exception if there is a problem
    data_dict.clear()
    connect_sql = True if sql_client is None else False
    try:
        # determine the sub group list

        if isinstance(employer_sub_group_list, int):
            filter_group_list = [employer_sub_group_list]
        else:
            filter_group_list = employer_sub_group_list

        # now determine the group list that will be passed to the stored proc
        logger.info("Reading Mongo Collection Data...")
        global mongo_collection
        if employer_sub_group_list is not None:
            conditions = {"data.group": int(employer_group_id), "data.sub_group": {"$in": employer_sub_group_list}}
        else:
            conditions = {"data.group": int(employer_group_id)}

        mongo_collection, results = mqa_comlib.find_customer_list(conditions, list_type)

        for doc in results:
            process_dest_row(doc)

        if connect_sql:
            logger.info("Connecting to SQL....")
            sql_client = comlib.connectToSqlSvr()

        # retrieve the rows as dictionary
        logger.info("Invoking Stored Proc...")
        cursor = sql_client.cursor(as_dict=True)

        statement = "EXEC dbo.PRC_GET_AEGF_SSN @MASTER_GROUP_ID =" + str(employer_group_id) + ",@SUB_GROUP_LIST = "
        if employer_sub_group_list is not None:
            group_list_comma = ",".join(map(str, employer_sub_group_list))
            statement = statement + "'" + group_list_comma + "'"
        else:
            statement = statement + "NULL"

        cursor.execute(statement)

        for row in cursor:
            process_source_row(employer_group_id, row)

        logger.info("Performing Updates...")
        for key, value in data_dict.items():
            try:
                comlib.process_list_update(mongo_collection, value)
            except Exception as rExc:
                logger.exception("Unable to Perform Update of SSN {ssn}".format(ssn=key), rExc)
        logger.info("Update Complete.")
    except Exception as e:
        logger.error("There was a an error Processing - " + str(e))
        raise e
    finally:
        # close the connections
        if connect_sql:
            sql_client.close()


def get_mapping_dict(employer_group_id, employer_sub_group):
    # routine to return a dictionary of the mapping needed for ssn - employee code for a given employer group and
    # optional sub-group this should be called by the generate process
    if employer_sub_group is not None:
        conditions = {"data.group": int(employer_group_id), "data.sub_group": int(employer_sub_group)}
    else:
        conditions = {"data.group": int(employer_group_id)}
        
    collection, results = mqa_comlib.find_customer_list(conditions, list_type)

    return_dict = {}
    
    for doc in results:
        value = None
        code = None
        if "emp_code" in doc["data"]:
            value = doc["data"]["emp_code"]
        if "ssn" in doc["data"]:
            code = doc["data"]["ssn"]
        if code is not None and value is not None:
            return_dict[code] = value

    return return_dict


def filter_master_groups(args):
    # callback invoked by the framework to get a list of master groups to process
    if args.master_group[0] == 'all':
        master_group_array = comlib.get_master_groups(False)
    else:
        sub_group_list = None
        if args.sub_groups:
            sub_group_list = args.sub_groups[0].split(",")
        master_group_array = [{"group": args.master_group[0], "sub_groups": sub_group_list}]
    return master_group_array


def main():
    # main routine that actually does the running as a standalone
    # parse the command line arguments
    parser = argparse.ArgumentParser(description="Routine to Sync the AEGF Employees from CRS")
    parser.add_argument("--master_group", nargs=1, help="Employee Master Group to Sync against or (all) for all",
                        required=True)
    parser.add_argument("--sub_groups", nargs=1, help="Employee Sub Group List (Comma Delimited)", required=False)
    parser.add_argument("--progress_id", nargs=1, help="Optional GTM CLI Progress Id", required=False)
    args = parser.parse_args()

    comlib.process_master_sync(logger, "SSN", args, filter_master_groups, perform_sync)


if __name__ == '__main__':
    main()
