import os
import sys, base64, time

try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
sys.path.insert(0, infoworks_python_dir)

from infoworks.core.mongo_utils import mongodb
from infoworks.core.iw_constants import CollectionName
import argparse, subprocess, json
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
parser.add_argument('--host', required=True, help="Provide the Databricks hostname")
parser.add_argument('--token', required=True, help="Provide the token")
parser.add_argument('--cluster_id', required=True, help="Provide the cluster ID here")
parser.add_argument('--param_file', required=True, help="Provide the file with the table details")
parser.add_argument('--mode', required=False, default="overwrite", help="Overwrite/Append")
parser.add_argument('--repartition', required=False, default=-1, help="repartition() is used for specifying the "
                                                                      "number of partitions considering the number of "
                                                                      "cores and the amount of data you have")

args = vars(parser.parse_args())

"""Update the metadata of the Databricks Tables to make them incremental"""


def update_metadata(obj_id, table_name, json_to_upd):
    try:
        mongodb[CollectionName.TABLES].update_one({'_id': obj_id}, {'$set': json_to_upd})
        print(bcolors.OKGREEN + "Metadata update done for {}".format(table_name) + bcolors.ENDC)
    except Exception as e:
        print(bcolors.FAIL + "Error while updating the metadata for table {}".format(obj_id) + bcolors.ENDC)
        print(bcolors.FAIL + str(e) + bcolors.ENDC)


def run_conversion_job(i, q):
    while True:
        try:
            print(bcolors.WARNING + '%s: Looking for the next conversion job' % i + bcolors.ENDC)
            params = q.get()
            # Do ur task here
            #source_name, table_name, source_schema, storage_format, last_ingested_cdc_value, last_merged_timestamp, sync_type, partition_column, override_columns, rowcount, source_type, sqoop_src_schema, sqoop_table_name, gen1_src_dir = params.split(',')
            infoworks_source_name, infoworks_target_schema, infoworks_table_name, hdi_dl_storage_format, last_ingested_cdc_value, last_merged_timestamp, hdi_src_dir, rowcount, partition_column, override_columns, source_type, hdi_src_schema, hdi_table_name, is_iw_table = params.split(',')
            #infoworks_source_name, infoworks_target_schema, infoworks_table_name, hdi_dl_storage_format, last_ingested_cdc_value, last_merged_timestamp, partition_column, override_columns, rowcount, source_type, hdi_src_schema, hdi_table_name, hdi_src_dir, is_iw_table = params.split(',')
            if hdi_src_schema == "" or hdi_table_name == "":
                hdi_src_schema = "N/A"
                hdi_table_name = "N/A"
            doc = mongodb[CollectionName.TABLES].find_one({"target_schema_name": infoworks_target_schema, "table": infoworks_table_name},
                                                          {"target_base_path": 1, "target_schema_name": 1, "table": 1,
                                                           "columns": 1})

            delta_dir = os.path.join(doc["target_base_path"], "merged")
            source_schema = doc["target_schema_name"]
            table_name = doc["table"]
            mode = args["mode"]
            if override_columns !="":
                override_columns = dict(item.split(":") for item in override_columns.split(","))
                override_columns_db = json.dumps(override_columns)
            else:
                override_columns_db = json.dumps({})
            if args['repartition'] == -1:
                cmd = "python pep_historical_table_migrate.py --host {} --token {} --cluster_id {} " \
                      "--ext_src_schema {} --ext_table_name {} --delta_dir {} --source_schema {} --table_name {} " \
                      "--mode {} --src_type {} " \
                      "--file_type {} --partition_by {} --override_columns {} --src_dir '{}' --is_iw_table {}".format(
                    args['host'], args['token'], args['cluster_id'], hdi_src_schema.strip(), hdi_table_name.strip() , delta_dir,
                    source_schema, table_name,
                    mode, source_type, hdi_dl_storage_format, "'" + partition_column + "'", "'" + override_columns_db + "'",hdi_src_dir.strip(),is_iw_table)
            else:
                cmd = "python pep_historical_table_migrate.py --host {} --token {} --cluster_id {} " \
                      "--ext_src_schema {} --ext_table_name {} --delta_dir {} --source_schema {} --table_name {} " \
                      "--mode {} --src_type {} " \
                      "--file_type {} --partition_by {} --override_columns {} --repartition {} --src_dir '{}' --is_iw_table {}".format(
                    args['host'], args['token'], args['cluster_id'], hdi_src_schema.strip(), hdi_table_name.strip() , delta_dir,
                    source_schema, table_name,
                    mode, source_type, hdi_dl_storage_format, "'" + partition_column + "'", "'" + override_columns_db + "'",
                    args['repartition'],hdi_src_dir.strip(),is_iw_table)

            print(bcolors.OKGREEN + str(i) + " : " + cmd + bcolors.ENDC)
            process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            animation = ["[■□□□□□□□□□]", "[■■□□□□□□□□]", "[■■■□□□□□□□]", "[■■■■□□□□□□]", "[■■■■■□□□□□]", "[■■■■■■□□□□]",
                         "[■■■■■■■□□□]", "[■■■■■■■■□□]", "[■■■■■■■■■□]", "[■■■■■■■■■■]"]
            for k in range(6):
                time.sleep(10)
                sys.stdout.write("\r" + animation[k % len(animation)])
                sys.stdout.flush()
            out, err = process.communicate()
            for k in [6, 7, 8, 9]:
                time.sleep(0.2)
                sys.stdout.write("\r" + animation[k % len(animation)])
                sys.stdout.flush()
            print(bcolors.OKBLUE + '\n%s: Output if any: ' % i + out.decode('utf-8') + bcolors.ENDC)
            print(bcolors.FAIL + '%s: Error if any: ' % i + err.decode('utf-8') + bcolors.ENDC)

            print(bcolors.HEADER + "%s: Updating the metadata now" % i + bcolors.ENDC)
            json_to_update = {"full_load_performed": True, "state": "ready", "rowCount": int(rowcount),
                              "last_ingested_cdc_value": last_ingested_cdc_value,
                              "last_merged_watermark": last_merged_timestamp, "state_export_data": False,
                              "state_fetch_delta": False, "state_merge_delta": False, "state_reconcile_data": False}

            update_metadata(doc['_id'], table_name, json_to_update)
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
    print(bcolors.WARNING + '*** Main thread waiting' + bcolors.ENDC)
    job_queue.join()
    print(bcolors.OKGREEN + '*** Done' + bcolors.ENDC)


main()
