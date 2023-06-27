"""
* scripts - generate_report.py
* Script to Generate the Actual Report
* This will be called by either the cron or the message listener for adhoc mode
* a) Generate the Report
* b) Output the Report to Mongo Grid FS
* c) Emit Logging Information
* d) if no pause on delivery - output the file to the filesystem and initiate MFT
*
* @author: MQAttach - Ravi
* Copyright 2008 MQAttach Canada Inc and all associated companies - All rights reserved
* Date: 2 June 2023
"""

import argparse
import logging
import pandas as pd
import csv
from datetime import datetime, timedelta
import sys
import os
from gridfs import GridFS
from colorama import Fore

import mqa_common as mqalib
import common_lib as comlib
import transform_lib as translib
import ssn_sync as ssnsync
import lob_sync as lobsync
import cron_lib as cronlib

# declare the definition object
report_definition = {}
report_id = ""
exec_date = datetime.now()

# declare the logger
#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
#logger = logging.getLogger()
logger = comlib.getLogger()

def get_source_data(conn):
    # retrieve the sql rows as dictionary
    master_group_arg = report_definition.get("group_id")
    sub_arg = ""
    status_arg = ""

    sub_group_id = report_definition.get("sub_group_id")
    if sub_group_id is not None:
        sub_arg = sub_group_id

    if report_definition["filter"].get("status") is not None:
        status_arg = ",".join(map(str, report_definition["filter"].get("status")))

    if report_definition["filter"].get("days") is None:
        report_definition["filter"]["days"] = 0

    days = int(report_definition["filter"]["days"])
    from_date = exec_date.date()
    to_date = from_date + timedelta(days=days)
    from_date_arg = str(from_date)
    to_date_arg = str(to_date)

    if to_date < from_date:
        from_date_arg = str(to_date)
        to_date_arg = str(from_date)

    # TODO: Remove this
    from_date_arg = "2010-05-01"

    aggregate_arg = 0
    if report_definition["output"]["aggregate"] is not None:
        aggregate_arg = 1 if report_definition["output"]["aggregate"] is True else 0

    cursor = conn.cursor(as_dict=True)

    '''
    statement = "EXEC dbo.PRC_GET_AEGF_POLICIES @MASTER_GROUP_ID=" + str(
        master_group_arg) + ",@SUB_GROUP_ID=" + sub_arg + ",@STATUS_LIST=N'" + status_arg + "', @FROM_DATE=N'" + from_date_arg + "', @TO_DATE=N'" + to_date_arg + "', @AGGREGATE=" + str(
        aggregate_arg)
    cursor.execute(statement)
    '''
    cursor.callproc('dbo.PRC_GET_AEGF_POLICIES', (master_group_arg, sub_arg, status_arg, from_date_arg, to_date_arg, aggregate_arg))
 
    return cursor


def parse_csv_options(options):
    # routine to return  the correct pandas options for csv output based on the given options
    params = {
        "index": False,
        "index_label": "Row No."  # this applies when index is True
    }
    if "exclude_header" in options:
        params["header"] = not options["exclude_header"]

    if "delimiter" in options:
        params["sep"] = options["delimiter"]

    if "text_delimiter" in options:
        params["quoting"] = csv.QUOTE_NONNUMERIC
        params["quotechar"] = options["text_delimiter"]

    return params


def store_grid_fs(path, meta):
    # routine to store the given path in gridfs with the given meta-data and return  the grid fs ID
    logger.info("Storing {path} in GRIDFS...".format(path=path))
    fs = GridFS(mqalib.get_mongo_db(), collection="AEGF_Binary")

    # set the meta-data
    meta["mqaModule"] = 9999
    meta["mqaModuleId"] = 900

    file_id = None
    with open(path, 'rb') as file:
        file_id = fs.put(file, filename=os.path.basename(path), metadata=meta)
    logger.info("Grids FS Record {id} Created...".format(id=file_id))
    return {"id": str(file_id), "type": os.path.splitext(path)[1][1:]}


def update_execution(progress_record, status: int, supplemental: str, base_dict: dict):
    # routine to update the execution
    try:
        if base_dict is None:
            base_dict = {}
        base_dict["date"] = exec_date
        base_dict["status"] = status
        base_dict["supplemental"] = supplemental
        comlib.update_execution(report_id, base_dict)

    except Exception as e:
        if progress_record is not None:
            mqalib.step_cli(progress_record, "Updating State", "Unable to update last execution " + str(e), 90)
        logger.error("Unable to Update Last Execution", e)


def create_output(result):
    # depending on the report output type take different actions
    output = report_definition["output"]
    if output.get("type") is None:
        output["type"] = 0
    if output.get("options") is None:
        output["options"] = {}

    logger.info("Creating Output Type = {type}...".format(type=output["type"]))
    options = report_definition["output"]["options"]

    cur_time = str(exec_date.strftime("%Y-%m-%dT%H.%M.%S"))

    output_parm = comlib.get_output_parameters()
    out_path = output_parm.temp_path + output_parm.prefix + "_{cur_time}".format(cur_time=cur_time)

    if options.get("exclude_header") is None:
        options["exclude_header"] = False

    df = pd.DataFrame(result)
    header = not options["exclude_header"]

    # perform the correct process based on output type
    if output["type"] == 0:
        # csv output
        csv_parms = parse_csv_options(options)
        out_path += ".csv"
        df.to_csv(out_path, **csv_parms)
    if output["type"] == 1:
        # xls output
        out_path += ".xlsx"
        df.to_excel(out_path, index=False, header=header)
    if output["type"] == 2:
        out_path += ".txt"
        df.to_csv(out_path, sep='\t', index=False, header=header)
    if output["type"] == 3:
        out_path += ".json"
        df.to_json(out_path, orient='records')

    logger.info("Output Created in Path {path}...".format(path=out_path))
    return out_path


def transform_func(value, *args, **kwargs):
    return value


def main():
    # parse the command line arguments
    parser = argparse.ArgumentParser(description="Routine to The Actual Report for AEGF based on the Definition Parameters")
    parser.add_argument("--id", nargs=1, help="Report Definition Identifier", required=True)
    parser.add_argument("--progress_id", nargs=1, help="Optional GTM CLI Progress Id", required=False)
    parser.add_argument("--adhoc", nargs=1, help="Optional Adhoc Generation", required=False)

    args = parser.parse_args()

    global report_id
    report_id = args.id[0]
    nextRun = None

    # checking if this is one time report generation (ex. following holiday)
    # cron job entry for one time report generation should be deleted after the job is run
    cront = cronlib.getCron()

    if "_" in report_id:
        # delete the job after one time report generation
        logger.info(f"Deleting cronjob entry created for one time report generation for id {report_id} ...")
        cronlib.delCronJob(cront, report_id, logger)

        report_id = report_id.split("_")[0]
    else:
        # getting the next run of the job
        nextRun = cronlib.nextRunOfCronjob(cront, report_id, logger)

    logger.info(f"Report Generation for Id {report_id} Started...")

    # initialize variables
    sql_client = None
    update_progress = False
    progress_record = None

    try:
        # initialize
        logger.info("Initializing Connection to Mongo ...")
        mongo_client, mongo_db = comlib.connectMongo()
        mqalib.init(mongo_client, mongo_db)

        if nextRun is not None:
            comlib.update_nextrun(report_id, nextRun)

        # check for a progress record
        if args.progress_id is not None:
            #print("CLI Invocation with Progress Id {id}...".format(id=args.progress_id[0]))
            logger.info("CLI Invocation - Reading Progress Id {id}".format(id=args.progress_id[0]))
            progress_record, update_progress = mqalib.read_progress(args.progress_id[0])

        # get the report definition
        logger.info(f"Retrieving Report Definition {report_id} ...")

        if update_progress:
            mqalib.step_cli(progress_record, "Retrieving Report Definition ...", None, 20)
        
        global report_definition
        # call rest api to get report definition
        report_definition = comlib.get_definition(report_id)
        
        if report_definition is None:
            raise ValueError("Report Definition is Empty")

        # check if the report is to be run or in adhoc mode
        if report_definition.get("holiday") and args.adhoc is None:
            raise ValueError("Run aborted - Holiday")

        # connect to SQL
        logger.info("Connecting to SQL ...")
        if update_progress:
            mqalib.step_cli(progress_record, "Connecting to SQL...", None, 22)

        sql_client = comlib.connectToSqlSvr()

        # retrieve the data
        logger.info("Retrieving Source Data ...")
        if update_progress:
            mqalib.step_cli(progress_record, "Retrieving Source Data...", None, 25)
        cursor = get_source_data(sql_client)

        # get all rows
        rows = cursor.fetchall()
        row_count = len(rows)

        logger.info("Found {row_count} Rows, Parsing into Data Frame...".format(row_count=row_count))
        if update_progress:
            mqalib.step_cli(progress_record, "Found {row_count} Rows".format(row_count=row_count),
                                "Parsing into Data Frame...", 25)

        df = pd.DataFrame(rows)
        column_arr = report_definition["columns"]
        total = len(column_arr)

        # initialize report variables
        result = {}
        count = 0
        errors = 0
        perc = 0

        ssn_xref = ssnsync.get_mapping_dict(report_definition.get("group_id"), report_definition.get("sub_group_id"))
        lob_xref = lobsync.get_mapping_dict(report_definition.get("group_id"))

        # loop thru the report definition columns
        for column in column_arr:
            count += 1
            try:
                if column["hidden"] is True:
                    continue

                # update the progress
                caption = column["caption"]
                desc = "Processing Column {caption}...".format(caption=caption)
                print(desc)
                perc = 10 + (60 * count / total)
                if update_progress:
                    mqalib.step_cli(progress_record, "Processing...", desc, perc)

                format_str = ""
                for src in column["data_source"]:
                    if src[0] == "$":
                        # if column is LOB and if transformation has lob or deduction lookup
                        if src == "$LOB$" and ("transformations" in column and any(trans["function"] in ["lob_lookup", "deduction_lookup"] for trans in column["transformations"] if "function" in trans)):
                            format_str = format_str + "{0[" + src[1:-1] + "]}" + "_{0[CARRIERADMINSYSTEMID]}"
                        else:
                            format_str = format_str + "{0[" + src[1:-1] + "]}"
                    else:
                        format_str = format_str + src

                result[caption] = df.agg(format_str.format, axis=1).apply(lambda val: translib.transform(val, column, ssn_xref, lob_xref))

            except Exception as e:
                errors += 1
                error_msg = "There was an error processing column {caption}...".format(
                    caption=caption)
                print(error_msg, file=sys.stderr)
                logger.exception(error_msg, e)
                if update_progress:
                    mqalib.step_cli(progress_record, "Processing...", error_msg, perc)
        if errors > 0:
            raise ValueError("There were errors encountered - Check the Log")

        logger.info("Report Column Generation Complete")
        if update_progress:
            mqalib.append_cli_step(progress_record, "Report Column Generation Complete")
            mqalib.step_cli(progress_record, "Producing Output...", "Creating Output File...", 85)

        file = create_output(result)
        if update_progress:
            mqalib.append_cli_step(progress_record, "Output File Generated - Path " + file + "...")
            mqalib.step_cli(progress_record, "Storing...", "Creating GridFS Record...", 90)

        store_result = store_grid_fs(file, {})
        update_execution(progress_record, 1, "Report Generated Successfully", {"payload_id": store_result["id"]})

        if update_progress:
            progress_record["info"]["result"] = store_result
            mqalib.step_cli(progress_record, "Storing...", "Grid FS Id " + store_result["id"], 90)
            mqalib.complete_cli(progress_record)

        final_result_msg = "Report Generation Complete - Report Stored in {file}".format(file=file)
        logger.info(final_result_msg)
        print(final_result_msg)

    except Exception as e:
        logger.error(f"{Fore.RED}Encountered error while generating report: {str(e)}{Fore.BLACK}")

        update_execution(progress_record, 0, "Report Failed " + str(e), None)
        if update_progress:
            mqalib.fail_cli(progress_record, e)
            print("Error Processing: {e}...".format(e=str(e)), file=sys.stderr)
    finally:
        # close the connections
        mqalib.destroy()

        if sql_client is not None:
            sql_client.close()

if __name__ == '__main__':
    main()
