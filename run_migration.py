import os
import sys,base64,time
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

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

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
        print(bcolors.OKGREEN+"Metadata update done for {}".format(table_name)+bcolors.ENDC)
    except Exception as e:
        print(bcolors.FAIL+"Error while updating the metadata for table {}".format(obj_id)+bcolors.ENDC)
        print(bcolors.FAIL+str(e)+bcolors.ENDC)

def run_conversion_job(i, q):
    while True:
        try:
            print(bcolors.WARNING+'%s: Looking for the next conversion job' % i+bcolors.ENDC)
            params = q.get()
            # Do ur task here
            table_id,table_name,storage_format,src_dir,source_id,last_ingested_cdc_value,last_merged_timestamp,sync_type,partition_column,override_columns_hdi,rowcount,source_name,hive_schema,source_type,sourceSubtype = params.split('|')
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

            override_columns_hdi = override_columns_hdi.encode("UTF-8")
            d = base64.b32decode(override_columns_hdi)
            override_columns_hdi_decoded = d.decode("UTF-8")
            override_columns_hdi = json.loads(override_columns_hdi_decoded)
            override_columns_hdi_keys = override_columns_hdi.keys()

            output_override = {}
            for key in override_columns_db.keys():
                if key in override_columns_hdi_keys:
                    output_override[override_columns_hdi[key]] = override_columns_db[key]
                else:
                    output_override[key] = override_columns_db[key]

            override_columns_db_str = json.dumps(output_override)

            partition_column = partition_column.encode("UTF-8")
            d = base64.b32decode(partition_column)
            partition_column_decoded = d.decode("UTF-8")
            partition_column = partition_column_decoded

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

            print(bcolors.OKGREEN+str(i) + " : "+cmd+bcolors.ENDC)
            process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            animation = ["[■□□□□□□□□□]", "[■■□□□□□□□□]", "[■■■□□□□□□□]", "[■■■■□□□□□□]", "[■■■■■□□□□□]", "[■■■■■■□□□□]",
                         "[■■■■■■■□□□]", "[■■■■■■■■□□]", "[■■■■■■■■■□]", "[■■■■■■■■■■]"]
            for k in range(6):
                time.sleep(10)
                sys.stdout.write("\r" + animation[k % len(animation)])
                sys.stdout.flush()
            out, err = process.communicate()
            for k in [6,7,8,9]:
                time.sleep(0.2)
                sys.stdout.write("\r" + animation[k % len(animation)])
                sys.stdout.flush()
            print(bcolors.OKBLUE+'\n%s: Output if any: ' %i + out.decode('utf-8')+bcolors.ENDC)
            print(bcolors.FAIL+'%s: Error if any: ' %i + err.decode('utf-8')+bcolors.ENDC)

            print(bcolors.HEADER+"%s: Updating the metadata now" %i +bcolors.ENDC)
            json_to_update = {"full_load_performed": True, "state": "ready", "rowCount": int(rowcount),
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
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        os.chdir(dname)
        with open(args["param_file"]) as fp:
            next(fp)
            for line in fp:
                job_queue.put(line)
    except Exception as e:
        print(str(e))

    # Now wait for the queue to be empty, indicating that we have processed all of the tables.
    print(bcolors.WARNING+'*** Main thread waiting'+bcolors.ENDC)
    job_queue.join()
    print(bcolors.OKGREEN+'*** Done'+bcolors.ENDC)

main()

