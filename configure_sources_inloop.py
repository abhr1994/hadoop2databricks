from os import listdir
import os, re, subprocess, time

source_jsons = listdir("/home/infoworks/abhi/hadoop2databricks/configs/source_jsons")
for source in source_jsons:
    matchObj = re.match(r'source_(.*).json', source)
    source_name = matchObj.group(1)
    print("######################Start of Source Configure##########################")
    print(source_name)
    source_json_path = os.path.join("/home/infoworks/abhi/hadoop2databricks/configs/source_jsons", source)
    cmd = "source /opt/infoworks/bin/env.sh;python /home/infoworks/abhi/hadoop2databricks/source_migration.py " \
          "--configuration_json_path {} --source_name {} --source_type rdbms --host_name localhost --host_port 2999 " \
          "--auth_token ytWtzxFzXlmmA90eDTXkWnrnVPq3IZA3jRLIWxwnaQ%2Bje9mbi4m10KJWFrTaGYXcu5earU0xLKvrLOVxdzNMkA== " \
          "--cluster_template job_default".format(
        source_json_path, source_name)
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    print('%s: Output if any: ' % source_name + out.decode('utf-8'))
    print('%s: Error if any: ' % source_name + err.decode('utf-8'))
    print("#######################End of Source Configure##########################\n")
    #time.sleep(10)
