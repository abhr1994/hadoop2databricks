import os,sys, csv

try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
sys.path.insert(0, infoworks_python_dir)

from infoworks.core.mongo_utils import mongodb
from infoworks.core.iw_constants import CollectionName
import argparse


def get_table_id(target_db_name,target_table_name):
    doc = mongodb[CollectionName.TABLES].find_one({"target_schema_name": target_db_name, "table": target_table_name},
                                                  {"target_base_path": 1, "target_schema_name": 1, "table": 1, })
    return doc['_id']


def update_metadata(obj_id, table_name, json_to_upd):
    try:
        mongodb[CollectionName.TABLES].update_one({'_id': obj_id}, {'$set': json_to_upd})
        print("Metadata update done for {}".format(table_name))
    except Exception as e:
        print("Error while updating the metadata for table {}".format(obj_id))

parser = argparse.ArgumentParser()
parser.add_argument('--param_file', required=True, help="Provide the param file here")
args = vars(parser.parse_args())

with open(args["param_file"], 'r') as data:
    for line in csv.DictReader(data):
        table_inputs = dict(line)
        target_db_name = table_inputs["infoworks_target_schema"]
        target_table_name = table_inputs["infoworks_table_name"]
        rowcount = int(table_inputs["rowcount"])
        last_ingested_cdc_value = table_inputs["last_ingested_cdc_value"]
        last_merged_timestamp = table_inputs["last_merged_timestamp"]
        table_id = get_table_id(target_db_name, target_table_name)
        json_to_update = {"full_load_performed": True, "state": "ready", "rowCount": rowcount,
                              "last_ingested_cdc_value": last_ingested_cdc_value,
                              "last_merged_watermark": last_merged_timestamp, "state_export_data": False,
                              "state_fetch_delta": False, "state_merge_delta": False, "state_reconcile_data": False}

        update_metadata(table_id, target_table_name, json_to_update)