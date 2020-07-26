#!/usr/bin/env python

__author__ = "Abhishek Raviprasad"
__version__ = "0.1.0"
__maintainer__ = "Abhishek Raviprasad"
__email__ = "abhishek.raviprasad@infoworks.io"
__status__ = "Dev"
__description__ = "Script to get the RDBMS source dump from IWX Hadoop product"


from bson import ObjectId

import os
import sys
import csv

try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
sys.path.insert(0, infoworks_python_dir)

from infoworks.core.mongo_utils import mongodb
from infoworks.core.iw_constants import CollectionName


def get_dump(source_id):
    try:
        src_id = ObjectId(source_id)
        tables_doc = mongodb[CollectionName.SOURCES].find_one({"_id":src_id},{"connection":1,"hive_schema":1,"sourceType":1,"sourceSubtype":1,"name":1,"hdfs_path":1,"isPublic":1})
        if not tables_doc:
            return None
        return tables_doc
    except Exception as e:
        print('Error: ' + str(e))
        return None

def get_sql_sources():
    try:
        src_list = ["sqlserver","teradata","oracle","netezza"]
        sql_sources = mongodb[CollectionName.SOURCES].find({ "$and": [{ "sourceType": "rdbms" }, { "sourceSubtype": { "$in": src_list } } ] }, {"_id"})
        source_list = [str(i['_id']) for i in list(sql_sources)]
        return source_list
    except Exception as e:
        print(str(e))
        return None

if __name__ == '__main__':
    source_list = get_sql_sources()
    header_order = ["connection.driver_name","connection.schema","connection.database","connection.connection_method","connection.connection_string","connection.username","connection.password","hive_schema","sourceType","sourceSubtype","name","hdfs_path","isPublic"]
    with open('/tmp/source_file.csv', mode='w') as source_file:
        source_writer = csv.writer(source_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        source_writer.writerow(header_order)
        for source in source_list:
            source_row = get_dump(source)
            row = []
            try:
                for i in header_order:
                    if i.startswith("connection"):
                        row.append(source_row.get("connection")[i.split(".")[-1]])
                    else:
                        row.append(source_row.get(i))
                source_writer.writerow(row)
            except Exception as e:
                print("Invalid source settings ",source_row)
                pass
