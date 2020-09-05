#!/usr/bin/env python

__author__ = "Abhishek Raviprasad"
__version__ = "0.1.0"
__maintainer__ = "Abhishek Raviprasad"
__email__ = "abhishek.raviprasad@infoworks.io"
__status__ = "Dev"
__description__ = "Script to dump the table details which are configured for CDC from IWX Hadoop product"


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

#Source ID can be list of src_ids
def dump_table_info(src_id = None):
    try:
        if src_id:
            src_id_list = [ObjectId(i) for i in src_id.split(',')]
            docs = mongodb[CollectionName.TABLES].find({"$and": [{"state": "ready"},
                                                      {"source": {"$in": src_id_list}}, {
                                                          "ingestion_configuration.sync_type": {
                                                              "$in": ["cdc-timestamp-column", "cdc-batch-id", "full-load"]}}]},
                                            {"_id": 1, "source": 1, "table": 1, "hdfs_path": 1,
                                             "ingestion_configuration": 1, "last_ingested_cdc_value": 1,
                                             "last_merged_timestamp": 1, "columns": 1})
        else:
            docs = mongodb[CollectionName.TABLES].find({"$and":[{"state":"ready"},{"ingestion_configuration.sync_type":{"$in":["cdc-timestamp-column","cdc-batch-id", "full-load"]}}]},{"_id":1,"source":1,"table":1,"hdfs_path":1,"ingestion_configuration":1,"last_ingested_cdc_value":1,"last_merged_timestamp":1,"columns":1})

        source_ids = []
        with open('/tmp/tables_params_file.csv', mode='w') as param_file:
            file_writer = csv.writer(param_file, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            file_writer.writerow(['table_id', 'table_name', 'storage_format', 'src_dir', 'source_id', 'last_ingested_cdc_value', 'last_merged_timestamp', 'sync_type','partition_column','override_columns'])
            for doc in docs:
                arr = doc["ingestion_configuration"].get("partition_key", [])
                partitions = {"normal":[],"drop" : []}
                for element in arr:
                    if element['partition_key_extract_from_col'] == True:
                        der_func = element['partition_key_derive_function']
                        if der_func != "regex":
                            partition_column = element['partition_key_derive_column']
                            partitions['normal'].append(partition_column)
                        else:
                            partition_column = element['partition_key_derive_column']
                            partitions['drop'].append(partition_column)
                    else:
                        partitions['normal'].append(element['partition_key_column'])
                partition_cols = json.dumps(partitions)

                override_columns = {}
                for src_column in doc["columns"]:
                    if src_column["origName"] != src_column["name"]:
                        orig_column = src_column["origName"]
                        renamed_column = src_column["name"]
                        override_columns[orig_column] = renamed_column
                override_columns_str = json.dumps(override_columns)
                row = [str(doc["_id"]),doc['table'],doc['ingestion_configuration']['storage_format'],doc["hdfs_path"],str(doc['source']),doc['last_ingested_cdc_value'],str(doc['last_merged_timestamp']),doc['ingestion_configuration']['sync_type'],partition_cols,override_columns_str]
                if not src_id:
                    source_ids.append(str(doc['source']))
                file_writer.writerow(row)
    except Exception as e:
        print(str(e))
        sys.exit(1)

    if not src_id:
        return source_ids
    else:
        return src_id.split(',')

def dump_source_info(src_ids):
    try:
        with open('/tmp/source_params_file.csv', mode='w') as src_dump_file:
            file_writer = csv.writer(src_dump_file, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            file_writer.writerow(['src_id','source_name','hive_schema','source_type','sourceSubtype'])
            for item in src_ids:
                src_id = ObjectId(item)
                doc = mongodb[CollectionName.SOURCES].find_one({"_id":src_id},{"name":1,"hive_schema":1,"sourceType":1,"sourceSubtype":1})
                row = [src_id,doc['name'],doc['hive_schema'],doc['sourceType'],doc['sourceSubtype']]
                file_writer.writerow(row)
    except Exception as e:
        print(str(e))
        sys.exit(1)

if len(sys.argv) == 2:
    source_ids = dump_table_info(sys.argv[1])
else:
    print("Ignoring any commandline arguments")
    source_ids = dump_table_info()

dump_source_info(list(set(source_ids)))

df1 = pd.read_csv("/tmp/tables_params_file.csv",delimiter="|",header='infer')
df2 = pd.read_csv("/tmp/source_params_file.csv",delimiter="|",header='infer')

result = pd.merge(df1, df2, how='inner', left_on = 'source_id', right_on = 'src_id')
result = result.drop(['src_id',],axis=1)
result.to_csv('/tmp/migration_parameter_file.csv', header=True, index=False,sep = "|")