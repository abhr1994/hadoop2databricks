import argparse
import requests
import json
import sys
import time

class Databricks(object):

    def __init__(self, **kwargs):
        self.host = kwargs['host'] if 'host' in kwargs else None
        self.token = kwargs['token'] if 'token' in kwargs else None

    def progress(self, _cur, _max):
        p = round(100*_cur/_max)
        b = f"Progress: {_cur}/{_max}"
        print(b, end="\r")

    def collect_jobs(self):
        host=self.host
        token=self.token

        jobs_list=requests.get("{db_url}/api/2.0/jobs/list".format(db_url=host),headers={"Authorization":"Bearer {bearer_token}".format(bearer_token=token),"Content-Type": "application/json"})
        jobs=jobs_list.json()['jobs']
        job_ids = [job['job_id'] for job in jobs]
        job_ids.sort(reverse=True)
        output_file = "/tmp/delete_jobs_new.txt"
        fd=open(output_file,'w')
        print("job_id,status",file=fd)
        job_num=0
        job_max=len(job_ids)

        for id in job_ids:
            job_runs=requests.get("{db_url}/api/2.0/jobs/runs/list?job_id={jobid}&active_only=true".format(db_url=host,jobid=id),headers={"Authorization":"Bearer {bearer_token}".format(bearer_token=token),"Content-Type": "application/json"})
            if job_runs.status_code == 200 and "runs" in job_runs.json() :
                print("Job "+str(id)+ " is active. So not deleting this job")
            else:
                data={"job_id":"{job_id}".format(job_id=id)}
                r=requests.post("{db_url}/api/2.0/jobs/delete".format(db_url=host),headers={"Authorization":"Bearer {bearer_token}".format(bearer_token=token),"Content-Type": "application/json"},json=data)
                print("{job_id},{status}".format(job_id=id,status=r.status_code),file=fd)
                self.progress(job_num,job_max)
                job_num += 1

        print("..."*5, end="\r")
        print("Done")
        fd.close()

class Input(object):
    def __init__(self):
        pass

    def get(self):
        parser = argparse.ArgumentParser(description='Delete databricks Jobs')
        parser.add_argument('-s', '--host', dest='host', required=True, help="Databricks Server URL")
        parser.add_argument('-t', '--token', dest='token', required=True, help="Databricks User Token")

        parse_input = parser.parse_args()
        if not parse_input.host or not parse_input.token:
            print("Databricks credentials not provided")
            parser.print_help()
            sys.exit(1)

        return parse_input


if __name__ == '__main__':
    input = Input()
    parse_input = input.get()
    dbObj=Databricks(host=parse_input.host,token=parse_input.token)
    dbObj.collect_jobs()