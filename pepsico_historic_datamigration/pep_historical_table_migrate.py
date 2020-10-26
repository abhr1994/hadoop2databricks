from databricks_api import DatabricksAPI
import argparse, logging, time, os, sys

try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)

parser = argparse.ArgumentParser()

parser.add_argument('--host', required=True, help="Provide the Databricks hostname")
parser.add_argument('--token', required=True, help="Provide the token")
parser.add_argument('--cluster_id', required=True, help="Provide the cluster ID here")
parser.add_argument('--delta_dir', required=True, help="Provide the target DBFS directory")
parser.add_argument('--source_schema', required=True, help="Provide the source schema name here ")
parser.add_argument('--table_name', required=True, help="Provide the table name here")
parser.add_argument('--mode', required=True, help="Overwrite/Append")
parser.add_argument('--logfile', required=False, default="migration.log", help="Log location")
parser.add_argument('--src_type', required=True, help="RDBMS/File")
parser.add_argument('--file_type', required=True, help="ORC/Parquet")
parser.add_argument('--ext_src_schema', required=True, help="Schema of the table which is residing in ADLS Gen1")
parser.add_argument('--ext_table_name', required=True, help="Name of the table which is residing in ADLS Gen1")
parser.add_argument('--src_dir', required=False, help="Source directory")
parser.add_argument('--partition_by', required=True, help="Comma seperated columns")
parser.add_argument('--is_iw_table', default="true", help="Is IW Table?")
parser.add_argument('--override_columns', required=False,
                    help="List of columns to over-ride. Eg: '{'fullname': 'fullname2', 'ins__ts': 'ins_ts3', 'abhi': 'abhis'}'")
parser.add_argument('--repartition', required=False, default=-1, help="repartition() is used for specifying the "
                                                                      "number of partitions considering the number of "
                                                                      "cores and the amount of data you have")
args = vars(parser.parse_args())
logging.basicConfig(filename='/opt/infoworks/temp/' + args['logfile'].strip(), filemode='a', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

try:
    db = DatabricksAPI(host=args['host'], token=args['token'])
    os.system("dbfs cp spark_defaults.conf dbfs:/FileStore/tables/confs/spark_defaults.conf")
    os.system("dbfs rm dbfs:/FileStore/tables/pep_delta_conversion.py")
    os.system("dbfs cp /home/infoworks/abhi/hadoop2databricks/pepsico_historic_datamigration/pep_delta_conversion.py dbfs:/FileStore/tables/pep_delta_conversion.py")
    job_name = args['file_type'] + " to Delta - Migration: "+ args["source_schema"]+ " : "+ args["table_name"]
    job_id = db.jobs.create_job(name=job_name, existing_cluster_id=args['cluster_id'], timeout_seconds=1800,
                                spark_python_task={"python_file": "dbfs:/FileStore/tables/pep_delta_conversion.py",
                                                   "parameters": None})
    # {'job_id': 47}
    if job_id.get('job_id', None):
        params = [args["ext_src_schema"],args["ext_table_name"], args["delta_dir"], args["mode"], args["source_schema"], args["table_name"],
                  args["src_type"], args["file_type"], args['partition_by'], args['override_columns'], args['repartition'], args["src_dir"], args["is_iw_table"]]
        run_details = db.jobs.run_now(job_id=job_id.get('job_id'), python_params=params)
        # {'run_id': 47, 'number_in_job': 1}
        run_id = run_details["run_id"]
        while True:
            response = db.jobs.get_run(run_id=run_id)
            state_message = response["state"].get("state_message", None)
            result_state = response["state"].get("result_state", None)
            life_cycle_state = response["state"].get("life_cycle_state", None)
            if (life_cycle_state is not None) and (
                    life_cycle_state in ['SUCCESS', 'FAILED', 'TIMEDOUT', 'CANCELED', 'TERMINATING', 'TERMINATED',
                                         'SKIPPED', 'INTERNAL_ERROR']):
                if result_state == 'SUCCESS':
                    print("Job is successfull")
                    logging.info("Job is successfull")
                    break
                else:
                    print("Job failed :" + state_message)
                    logging.error("Job failed :" + state_message)
                    raise Exception("Job failed :" + state_message)
            else:
                print("Job is still running with state: ", life_cycle_state, " : ", state_message)
                logging.info("Job is still running with state: " + life_cycle_state + " : " + state_message)
                time.sleep(2)
except Exception as e:
    raise Exception(str(e))