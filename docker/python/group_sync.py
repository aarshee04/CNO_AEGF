"""
* scripts - group_sync.py
* Script to Manage the Syncing of the Employer Group List from the Source CRS System
* This script should only be invoked by the UI through the listener
*
* @author: MQAttach - Mac Bhyat
* Copyright 2008 MQAttach Canada Inc and all associated companies - All rights reserved
* Date: 14 June 2023
"""
import sys
import argparse
import logging

import common_lib as comlib
import mqa_common as mqa_comlib

# declare the dictionary that will hold the data that needs to be looked up against and eventually written to mongo
data_dict = {}
mongo_collection = None
list_type = comlib.get_list_types()["group"]

# declare the logger
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


def perform_mapping(dest_row, source_row):
    # routine to update the fields on the given destination with data from the given source
    delete_flag = source_row["DELETEFLAG"]
    dest_row["description"] = source_row["GROUPNAME"]
    dest_row["status"] = 1 if delete_flag == 0 else 0
    if source_row["GROUPNUMBER"] is not None:
        dest_row["crs_number"] = source_row["GROUPNUMBER"]
    dest_row["carrier_id"] = source_row["CARRIERID"]
    return dest_row


def marshall_sub_groups(sub_groups):
    # routine to perform marhalling of the sub-groups from dictionary to array prior to write
    return_dict = []
    for key, value in sub_groups.items():
        return_dict.append(value)
    return return_dict


def process_master_group(source_row, group_id):
    # routine to process a master group into the dictionary
    group_id = str(group_id)
    delete_flag = source_row["DELETEFLAG"]
    if group_id in data_dict:
        obj = data_dict[group_id]

    else:
        if delete_flag == 1:
            return

        # add this master group as a new record
        obj = mqa_comlib.create_customer_list_record(group_id, source_row["GROUPNAME"], list_type)
        obj["sub_group_dict"] = {}
        data_dict[group_id] = obj

    # update the object
    obj["data"] = perform_mapping(obj["data"], source_row)
    return


def process_sub_group(row, master_group):
    # routine to process a sub group record into the dictionary
    delete_flag = row["DELETEFLAG"]
    master_group = str(master_group)
    group_id = str(row["GROUPID"])
    if master_group not in data_dict:
        process_master_group(row, master_group)
    if master_group not in data_dict:
        return  # should never happen

    # get the master group
    master_obj = data_dict[master_group]

    # get the sub group dictionary
    sub_group_dict = master_obj["sub_group_dict"]
    if sub_group_dict is None:
        sub_group_dict = {}
    if group_id in sub_group_dict:
        obj = sub_group_dict[group_id]
    else:
        if delete_flag == 1:
            return
        # create a new sub group
        obj = {"code": group_id}
        sub_group_dict[group_id] = obj

    # update the object
    obj = perform_mapping(obj, row)
    return


def process_dest_row(row):
    # routine to unmarshall a mongo source record into the main dictionary
    sub_dict = {}
    if row["data"]["sub_groups"] is not None:
        for sub_group in row["data"]["sub_groups"]:
            sub_dict[sub_group["code"]] = sub_group
    row["sub_group_dict"] = sub_dict
    data_dict[row["code"]] = row


def process_source_row(row):
    # routine process a source row
    master_group = row["MASTERGROUPID"]
    if master_group is None:
        process_master_group(row, row["GROUPID"])
    else:
        process_sub_group(row, master_group)


def process_dest_update(record):
    # routine to perform the write of the given dictionary

    # update the sub group list
    record["data"]["sub_groups"] = None
    if "sub_group_dict" in record:
        record["data"]["sub_groups"] = marshall_sub_groups(record["sub_group_dict"])
        del record["sub_group_dict"]

    # now perform the actual mongo update
    if record["_id"] is None:
        del record["_id"]
        result = mongo_collection.insert_one(record)
    else:
        result = mongo_collection.update_one({'_id': record["_id"]}, {"$currentDate": {"sys_date": True}, "$set": {
            'data': record["data"],
            'description': record["description"],
        }})
    return result


def main():
    # main routine that actually does the running as a standalone
    # parse the command line arguments
    parser = argparse.ArgumentParser(description="Routine to Sync the AEGF Employer Groups from CRS")
    parser.add_argument("--progress_id", nargs=1, help="Optional GTM CLI Progress Id", required=False)
    args = parser.parse_args()

    print("Group Sync Started...")

    # initialize variables
    sql_client = None
    progress_record = None
    update_progress = False
    try:
        # initialize
        logging.info("Initializing Connection to Mongo...")
        mongo_client, mongo_db = comlib.connectMongo()
        mqa_comlib.init(mongo_client, mongo_db)

        if args.progress_id:
            print("CLI Invocation with Progress Id {id}...".format(id=args.progress_id[0]))
            logger.info("CLI Invocation - Reading Progress Id {id}".format(id=args.progress_id[0]))
            progress_record, update_progress = mqa_comlib.read_progress(args.progress_id[0])

        # get the data from mongo and perform the unmarshalling
        if update_progress:
            mqa_comlib.step_cli(progress_record, "Reading Mongo Collection Data...", None, 15)

        logger.info("Reading Mongo Collection Data...")
        global mongo_collection
        mongo_collection, results = mqa_comlib.find_customer_list_for_type(list_type)

        # loop through the results and perform the dictionary unmarshalling
        for doc in results:
            process_dest_row(doc)

        # connect to SQL and run the proc
        if update_progress:
            progress_record = mqa_comlib.step_cli(progress_record, "Connecting to CRS Data...", None, 20)

        logger.info("Connecting to SQL....")
        sql_client = comlib.connectToSqlSvr()

        # retrieve the rows as dictionary
        logger.info("Invoking Stored Proc...")
        cursor = sql_client.cursor(as_dict=True)
        cursor.callproc('dbo.PRC_GET_AEGF_GROUPS')

        if update_progress:
            progress_record = mqa_comlib.step_cli(progress_record, "Merging CRS Data...", None, 25)
        for row in cursor:
            process_source_row(row)

        if update_progress:
            progress_record = mqa_comlib.step_cli(progress_record, "Performing Updates...", None, 80)
        logger.info("Performing Updates...")
        for key, value in data_dict.items():
            try:
                process_dest_update(value)
            except Exception as rExc:
                logger.exception("Unable to Perform Update of Master Group {group}".format(group=key), rExc)
        if update_progress:
            mqa_comlib.complete_cli(progress_record)

        logger.info("Update Complete.")
        print("Update Complete.")

    except Exception as e:
        print("Error Processing: {e}...".format(e=str(e)), file=sys.stderr)
        logger.error("There was a an error Processing - " + str(e))
        if update_progress:
            mqa_comlib.fail_cli(progress_record, e)
    finally:
        # close the connections
        mqa_comlib.destroy()
        if sql_client is not None:
            sql_client.close()


if __name__ == '__main__':
    main()
