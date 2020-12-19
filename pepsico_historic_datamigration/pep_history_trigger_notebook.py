import time, os
from databricks_api import DatabricksAPI
import argparse
import queue
from threading import Thread

num_fetch_threads = 5
job_queue = queue.Queue(maxsize=20)

parser = argparse.ArgumentParser()
parser.add_argument('--param_file', required=True, help="Pass the param_file here")
args = vars(parser.parse_args())


def trigger_notebook(i, q):
    while True:
        try:
            print('%s: Looking for the next conversion job' % i)
            params = q.get()
            source_schema, table_name, src_dir, delta_dir, mode, src_type = params.split(',')
            notebook_params = {"src_dir": src_dir, "delta_dir": delta_dir, "mode": mode, "source_schema": source_schema,
                               "table_name": table_name, "src_type": src_type}
            host = "adb-2055249590751045.5.azuredatabricks.net"
            token = "dapibd8a79ee92c83acf567a5fc59db69372"
            db = DatabricksAPI(host=host, token=token)
            '''
            jobs_body = {"name": "Wave3 Delta Conversion: " + table_name,
                         "new_cluster": {"spark_version": "7.3.x-scala2.12", "node_type_id": "Standard_DS13_v2",
                                         "driver_node_type_id": "Standard_DS3_v2", "num_workers": 2},
                         "email_notifications": {"on_start": [], "on_success": [], "on_failure": []},
                         "timeout_seconds": 36000, "max_retries": 1, "notebook_task": {
                    "notebook_path": "/Users/abhishek.raviprasad.contractor@pepsico.com/Wave3"}}
            '''
            job_id = db.jobs.create_job(name="Wave3 Delta Conversion: " + table_name,
                                        existing_cluster_id="1021-184115-rep117", timeout_seconds=36000, notebook_task={
                    "notebook_path": "/Users/abhishek.raviprasad.contractor@pepsico.com/Wave3"})
            print(job_id.get('job_id'))
            run_details = db.jobs.run_now(job_id=job_id.get('job_id'), notebook_params=notebook_params)
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
                        break
                    else:
                        print("Job failed :" + state_message)
                        db.jobs.delete_job(job_id.get('job_id'))
                else:
                    time.sleep(2)
            print('%s: Task Done' % i)
            print(table_name + " succesfull")
            db.jobs.delete_job(job_id.get('job_id'))
            q.task_done()
        except Exception as e:
            print(str(e))
            q.task_done()


def main():
    for i in range(num_fetch_threads):
        worker = Thread(target=trigger_notebook, args=(i, job_queue,))
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
    print('*** Main thread waiting')
    job_queue.join()
    print('*** Done')


main()
