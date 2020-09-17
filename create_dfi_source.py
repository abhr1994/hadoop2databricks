import requests, argparse, os, sys

try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)
infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
sys.path.insert(0, infoworks_python_dir)
from infoworks.core.iw_utils import IWUtils

parser = argparse.ArgumentParser('Create DFI Source')
parser.add_argument('--auth_token', default='AU66mMKxL9fEwHEUe9cMnlK9RTDNLkIfp7vcYdT9cSs=', required=False,
                    help='Valid authentication token.')
parser.add_argument('--source_name', required=True, help="Source name")
parser.add_argument('--target_schema_name', required=True, help="Schema name")
parser.add_argument('--target_base_path', required=True, help="Target DBFS Base Path")
parser.add_argument('--source_base_path', required=True, help="Source Base Path")
parser.add_argument('--source_file_location', required=True, help="blob/sftp")

parser.add_argument('--container', required=False, help="Container Name")
parser.add_argument('--account_name', required=False, help="Account Name")
parser.add_argument('--account_key', required=False, help="Account key")
parser.add_argument('--source_base_path_relative', required=False, help="Source Relative Path")

parser.add_argument('--host', required=False, help="SFTP Hostname")
parser.add_argument('--port', required=False, help="SFTP Port")
parser.add_argument('--username', required=False, help="SFTP username")
parser.add_argument('--password', required=False, help="SFTP password")

args = vars(parser.parse_args())

url = "http://localhost:2999/v1.1/source/create.json?auth_token={}".format(args['auth_token'])
print(url)

if args['source_file_location'].lower() == 'blob':
    temp = os.popen(
        "bash /opt/infoworks/apricot-meteor/infoworks_python/infoworks/bin/infoworks_security.sh -encrypt -p {}".format(args['account_key']))
    encrypted_key = temp.read().strip()
    payload = {"configuration": {"name": args['source_name'], "target_schema_name": args['target_schema_name'],
                                 "target_base_path": args['target_base_path'], "isPublic": True, "sourceType": "file",
                                 "sourceSubtype": "structured",
                                 "connection": {"fileType": "structured", "host_type": "cloud"},
                                 "source_base_path": args['source_base_path'],
                                 "cloud_settings": {"container": args['container'], "access_type": "account_key",
                                                    "cloud_type": "wasb", "account_name": args['account_name'],
                                                    "account_key": encrypted_key },
                                 "source_base_path_relative": args['source_base_path_relative']}}
    configuration = IWUtils.ejson_serialize(payload)

elif args['source_file_location'].lower() == 'sftp':
    temp = os.popen(
        "bash /opt/infoworks/apricot-meteor/infoworks_python/infoworks/bin/infoworks_security.sh -encrypt -p {}".format(
            args['password']))
    encrypted_password = temp.read().strip()
    payload = {"configuration": {"name": args['source_name'], "target_schema_name": args['target_schema_name'],
                                 "target_base_path": args['target_base_path'], "isPublic": True, "sourceType": "file",
                                 "sourceSubtype": "structured","connection": {"fileType": "structured", "host_type": "remote", "auth_type": "password",
                                                                              "host": args['host'], "password": encrypted_password, "port": args['port'], "username": args['username']},
                                 "source_base_path": args['source_base_path']}}
    configuration = IWUtils.ejson_serialize(payload)
else:
    sys.exit(1)

headers = {
    'Content-Type': 'application/json'
}
response = requests.request("POST", url, headers=headers, data=configuration)
print(response.text.encode('utf8'))
