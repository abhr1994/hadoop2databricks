import os
import sys
try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
sys.path.insert(0, infoworks_python_dir)

from infoworks.core.mongo_utils import mongodb
from infoworks.core.iw_constants import CollectionName
import argparse,subprocess,json
import queue
from threading import Thread

num_fetch_threads = 2
job_queue = queue.Queue(maxsize=20)

parser = argparse.ArgumentParser()
parser.add_argument('--host',required=True,help="Provide the Databricks hostname")
parser.add_argument('--token',required=True,help="Provide the token")
parser.add_argument('--cluster_id',required=True,help="Provide the cluster ID here")
parser.add_argument('--param_file',required=True,help="Provide the file with the table details")
parser.add_argument('--mode',required=False,default="overwrite",help="Overwrite/Append")
parser.add_argument('--repartition',required=False,default=-1,help="repartition() is used for specifying the "
                                                                      "number of partitions considering the number of "
                                                                      "cores and the amount of data you have")

args = vars(parser.parse_args())

"""Update the metadata of the Databricks Tables to make them incremental"""
def update_metadata(obj_id,table_name,json_to_upd):
    try:
        mongodb[CollectionName.TABLES].update_one({'_id': obj_id},{'$set': json_to_upd})
        print("Metadata update done for {}".format(table_name))
    except Exception as e:
        print("Error while updating the metadata for table {}".format(obj_id))
        print(str(e))

def run_conversion_job(i, q):
    try:
        while True:
            print('%s: Looking for the next conversion job' % i)
            params = q.get()
            # Do ur task here
            table_id,table_name,storage_format,src_dir,source_id,last_ingested_cdc_value,last_merged_timestamp,sync_type,partition_column,override_columns_hdi,source_name,hive_schema,source_type,sourceSubtype = params.split('|')
            doc = mongodb[CollectionName.TABLES].find_one({"target_schema_name": hive_schema, "table": table_name},
                                                      {"target_base_path": 1, "target_schema_name": 1, "table": 1,"columns":1})

            src_dir = os.path.join("/mnt/datafoundry/hdi_sources",src_dir.lstrip("/"))
            delta_dir = os.path.join(doc["target_base_path"], "merged")
            source_schema = doc["target_schema_name"]
            table_name = doc["table"]
            mode = args["mode"]

            override_columns_db = {}
            for src_column in doc["columns"]:
                if src_column["origName"] != src_column["name"]:
                    orig_column = src_column["origName"]
                    renamed_column = src_column["name"]
                    override_columns_db[orig_column] = renamed_column

            override_columns_hdi = json.loads(override_columns_hdi.replace('""', '"').replace('"[', '[').replace(']"', ']').replace('"{', '{').replace('}"', '}'))
            override_columns_hdi_keys = override_columns_hdi.keys()

            output_override = {}
            for key in override_columns_db.keys():
                if key in override_columns_hdi_keys:
                    output_override[override_columns_hdi[key]] = override_columns_db[key]
                else:
                    output_override[key] = override_columns_db[key]

            override_columns_db_str = json.dumps(output_override)
            partition_column = partition_column.replace('""', '"').replace('"[', '[').replace(']"', ']').replace('"{', '{').replace('}"', '}')

            if args['repartition'] == -1:
                cmd = "python historical_table_migrate.py --host {} --token {} --cluster_id {} " \
                      "--src_dir {} --delta_dir {} --source_schema {} --table_name {} --mode {} --src_type {} " \
                      "--file_type {} --partition_by {} --override_columns {}".format(
                    args['host'], args['token'], args['cluster_id'], src_dir, delta_dir, source_schema, table_name,
                    mode, source_type,storage_format, "'" + partition_column + "'", "'" + override_columns_db_str + "'")
            else:
                cmd = "python historical_table_migrate.py --host {} --token {} --cluster_id {} " \
                      "--src_dir {} --delta_dir {} --source_schema {} --table_name {} --mode {} --src_type {} " \
                      "--file_type {} --partition_by {} --override_columns {} --repartition {}".format(
                    args['host'], args['token'], args['cluster_id'], src_dir, delta_dir, source_schema, table_name,
                    mode, source_type,storage_format, "'" + partition_column + "'", "'" + override_columns_db_str + "'", args['repartition'])

            print(str(i) + " : "+cmd)
            process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = process.communicate()
            print('%s: Output if any: ' %i + out.decode('utf-8'))
            print('%s: Error if any: ' %i + err.decode('utf-8'))

            print("%s: Updating the metadata now" %i )
            json_to_update = {"full_load_performed": True, "state": "ready",
                              "last_ingested_cdc_value": last_ingested_cdc_value,
                              "last_merged_watermark": last_merged_timestamp, "state_export_data": False,
                              "state_fetch_delta": False, "state_merge_delta": False, "state_reconcile_data": False}

            update_metadata(doc['_id'],table_name,json_to_update)
            q.task_done()
    except Exception as e:
        print(str(e))
        q.task_done()


def main():
    for i in range(num_fetch_threads):
        worker = Thread(target=run_conversion_job, args=(i, job_queue,))
        worker.setDaemon(True)
        worker.start()
    try:
        with open(args["param_file"]) as fp:
            next(fp)
            for line in fp:
                job_queue.put(line)
    except Exception as e:
        print(str(e))

    # Now wait for the queue to be empty, indicating that we have processed all of the tables.
    print('*** Main thread waiting')
    job_queue.join()
    print('*** Done')

main()


'''
python run_migration.py --host adb-5996418727748488.8.azuredatabricks.net --token dapi6c880eecf5f80d33e31b4aa86d653a7c --cluster_id 0718-041317-gilt53 --param_file param_file.csv
'''