"""
* scripts - ssn_sync.py
* Script to Manage the Syncing of the Employees for a given Employer Group Code from the CRS Source System
* This script will be invoked by the UI or through an import during the generation process
*
* @author: MQAttach - Mac Bhyat
* Copyright 2008 MQAttach Canada Inc and all associated companies - All rights reserved
* Date: 16 June 2023
"""

import argparse
import logging
from bson import ObjectId

import common_lib as comlib
import mqa_common as mqa_comlib

# declare the dictionary that will hold the data that needs to be looked up against and eventually written to mongo
data_dict = {}
mongo_collection = None
list_type = comlib.get_list_types()["lob"]

# declare the logger
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


def process_dest_row(row):
    # routine to update the the mongo row into the dictionary
    code = "{product}_{carrier}".format(product=row["data"]["product_lob"], carrier=str(row["data"]["carrier_id"]))
    row["insert"] = False
    data_dict[code] = row


def process_source_row(employee_group_id, row):
    # routine to process the actual source row into the dictionary

    product = row["PRODUCTLOB"]
    carrier = row["CARRIERADMINSYSTEMID"]
    if product is None or carrier is None:
        return
    code = "{product}_{carrier}".format(product=product, carrier=str(carrier))
    if code in data_dict:
        obj = data_dict[code]
    else:
        # create a new record
        code = ObjectId()
        obj = mqa_comlib.create_customer_list_record(str(code), row["DESCRIPTION"], list_type)
        obj["data"] = {"product_lob": product, "group": employee_group_id, "carrier_id": carrier}
        obj["description"] = row["DESCRIPTION"]
        obj["_id"] = code
        obj["insert"] = True
        data_dict[code] = obj

    # update the group
    obj["data"]["group"] = employee_group_id

    # update the mapping code
    source_code = row["COVERAGETYPE"]
    if source_code is not None:
        obj["data"]["coverage_code"] = source_code
        dest_code = None if "mapping_code" not in obj["data"] else obj["data"]["mapping_code"]
        if dest_code is None:
            obj["data"]["mapping_code"] = source_code

    # update the description
    obj["data"]["crs_description"] = row["DESCRIPTION"]


def perform_sync(employer_group_id, sub_groups, sql_client):
    # routine to perform the sync or throw an exception if there is a problem
    data_dict.clear()
    connect_sql = True if sql_client is None else False
    try:
        # now determine the group list that will be passed to the stored proc
        logger.info("Reading Mongo Collection Data...")
        global mongo_collection
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

        statement = "EXEC dbo.PRC_GET_AEGF_LOB @MASTER_GROUP_ID =" + str(employer_group_id)

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


def get_mapping_dict(employer_group_id):
    # routine to return a dictionary of the mapping needed for product lob to coverrage for a given employer group
    # this should be called by the generate process
    conditions = {"data.group": int(employer_group_id)}
    collection, results = mqa_comlib.find_customer_list(conditions, list_type)

    return_dict = {}
    for doc in results:
        value = None
        code = None

        if "product_lob" in doc["data"] and "carrier_id" in doc["data"]:
            code = "{product}_{carrier}".format(product=doc["data"]["product_lob"],
                                                carrier=str(doc["data"]["carrier_id"]))

        value = {"code": doc["data"].get("mapping_code"), "description": doc.get("description"),
                 "deduction": doc["data"].get("deduction")}

        if code is not None and value["code"] is not None:
            return_dict[code] = value
            
    return return_dict


def filter_master_groups(args):
    # callback invoked by the framework to get a list of master groups to process
    if args.master_group[0] == 'all':
        master_group_array = comlib.get_master_groups(False)
    else:
        master_group_array = [{"group": args.master_group[0], "sub_groups": None}]
    return master_group_array


def main():
    # main routine that actually does the running as a standalone
    # parse the command line arguments
    parser = argparse.ArgumentParser(description="Routine to Sync the AEGF Coverage Types from CRS")
    parser.add_argument("--master_group", nargs=1, help="Employee Master Group to Sync against or (all) for all",
                        required=True)
    parser.add_argument("--progress_id", nargs=1, help="Optional GTM CLI Progress Id", required=False)
    args = parser.parse_args()

    comlib.process_master_sync(logger, "LOB", args, filter_master_groups, perform_sync)


if __name__ == '__main__':
    main()
