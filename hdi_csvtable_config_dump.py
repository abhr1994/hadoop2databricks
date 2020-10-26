#!/usr/bin/env python

__author__ = "Abhishek Raviprasad"
__version__ = "0.2.0"
__maintainer__ = "Abhishek Raviprasad"
__email__ = "abhishek.raviprasad@infoworks.io"
__status__ = "Dev"
__description__ = "Script to dump the table details which are configured for CDC/Full load from IWX Hadoop product"

import os
import sys
import csv
import json
import pandas as pd

try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
sys.path.insert(0, infoworks_python_dir)

from infoworks.core.mongo_utils import mongodb
from infoworks.core.iw_constants import CollectionName
from bson import ObjectId


def get_source_name(src_id):
    src_obj = mongodb[CollectionName.SOURCES].find_one({"_id": src_id}, {"name": 1})
    return src_obj["name"]

def dump_file_mappings(src_id=None):
    try:
        if src_id:
            src_id_list = [ObjectId(i) for i in src_id.split(',')]
            states = ["switching","ready"]
            docs = mongodb[CollectionName.TABLES].find({"$and": [{"state": {"$in": states}},
                                                                 {"source": {"$in": src_id_list}}, {
                                                                     "ingestion_configuration.sync_type": {
                                                                         "$in": ["cdc-timestamp-column", "cdc-batch-id",
                                                                                 "full-load"]}}]},
                                                       {"_id": 1, "source": 1, "table": 1,"ingestion_configuration": 1})
        else:
            states = ["switching", "ready"]
            docs = mongodb[CollectionName.TABLES].find({"$and": [{"state": {"$in": states}}, {
                "ingestion_configuration.sync_type": {"$in": ["cdc-timestamp-column", "cdc-batch-id", "full-load"]}}]},
                                                       {"_id": 1, "source": 1, "table": 1,"ingestion_configuration": 1})
        with open('/tmp/file_mappings.csv', mode='w') as param_file:
            file_writer = csv.writer(param_file, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            file_writer.writerow(
                ['target_table_name', 'source_relative_path', 'target_relative_path', 'exclude_filename_regex',
                 'include_filename_regex', 'sfp_column_enclosed_by', 'sfp_header_rows_count', 'sfp_column_separator'])
            for doc in docs:
                target_table_name = doc["ingestion_configuration"]["hive_table_name"]
                source_relative_path = doc["ingestion_configuration"]["source_relative_path"]
                target_relative_path = doc["ingestion_configuration"]["hdfs_relative_path"]
                exclude_filename_regex = doc["ingestion_configuration"]["exclude_filename_regex"]
                include_filename_regex = doc["ingestion_configuration"]["include_filename_regex"]
                sfp_column_enclosed_by = doc["ingestion_configuration"]["source_file_properties"]["column_enclosed_by"]
                sfp_header_rows_count = doc["ingestion_configuration"]["source_file_properties"]["header_rows_count"]
                sfp_column_separator = doc["ingestion_configuration"]["source_file_properties"]["column_separator"]
                row = [target_table_name, source_relative_path, target_relative_path, exclude_filename_regex,
                       include_filename_regex, sfp_column_enclosed_by, sfp_header_rows_count, sfp_column_separator]
                file_writer.writerow(row)
    except Exception as e:
        print(str(e))
        sys.exit(1)

# Source ID can be list of src_ids
def dump_table_info(src_id=None):
    try:
        if src_id:
            src_id_list = [ObjectId(i) for i in src_id.split(',')]
            states = ["switching", "ready"]
            docs = mongodb[CollectionName.TABLES].find({"$and": [{"state":  {"$in": states}},
                                                                 {"source": {"$in": src_id_list}}, {
                                                                     "ingestion_configuration.sync_type": {
                                                                         "$in": ["cdc-timestamp-column", "cdc-batch-id",
                                                                                 "full-load"]}}]},
                                                       {"_id": 1, "source": 1, "table": 1,"ingestion_configuration": 1})
        else:
            states = ["switching", "ready"]
            docs = mongodb[CollectionName.TABLES].find({"$and": [{"state":  {"$in": states}}, {
                "ingestion_configuration.sync_type": {"$in": ["cdc-timestamp-column", "cdc-batch-id", "full-load"]}}]},
                                                       {"_id": 1, "source": 1, "table": 1,"ingestion_configuration": 1})


        with open('/tmp/tables_params_file.csv', mode='w') as param_file:
            file_writer = csv.writer(param_file, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            file_writer.writerow(['source_name', 'target_table_name', 'sync_type', 'watermark_column', 'natural_keys', 'is_archive_enabled', 'split_by_key', 'generate_history_view', 'update_strategy', 'partition_keys'])
            for doc in docs:
                try:
                    src_name = get_source_name(doc["source"])
                    partition_key_temp = doc["ingestion_configuration"].get("partition_key", "N/A")
                    partition_keys = []
                    if partition_key_temp!="N/A":
                        for key in partition_key_temp:
                            partition_keys.append(key["partition_key_column"])
                        partition_keys = ",".join(partition_keys)
                    else:
                        partition_keys = "N/A"
                    natural_keys = doc['ingestion_configuration'].get('natural_key',"N/A")
                    if natural_keys != "N/A":
                        natural_keys = ",".join(natural_keys)
                    if doc['ingestion_configuration']['sync_type'] == "cdc-batch-id":
                        cdc_column = doc['ingestion_configuration']['batch_id_cdc_column']
                        sync_type = "incremental"
                        update_strategy = "merge"
                    elif doc['ingestion_configuration']['sync_type'] == "cdc-timestamp-column":
                        cdc_column = doc['ingestion_configuration']['timestamp_column_update']
                        sync_type = "incremental"
                        update_strategy = "merge"
                    else:
                        sync_type = "full-load"
                        cdc_column = "N/A"
                        update_strategy = "N/A"
                    row = [src_name, doc['table'],sync_type,cdc_column,natural_keys,"FALSE","N/A","FALSE",update_strategy,partition_keys]
                    file_writer.writerow(row)
                except Exception :
                    print("Skipping {} {}".format(src_name,doc['table']))
    except Exception as e:
        print(str(e))
        sys.exit(1)


if len(sys.argv) == 2:
    dump_table_info(sys.argv[1])
    dump_file_mappings(sys.argv[1])
else:
    print("Ignoring any commandline arguments")
    dump_table_info()
    dump_file_mappings()


