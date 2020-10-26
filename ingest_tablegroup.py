#!/usr/bin/env python
import os, requests, sys, logging, argparse, copy, csv

try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
sys.path.insert(0, infoworks_python_dir)
from infoworks.core.iw_utils import IWUtils
from infoworks.core.mongo_utils import mongodb
from infoworks.core.iw_constants import CollectionName

logging.getLogger().setLevel(logging.INFO)
parser = argparse.ArgumentParser('Ingest TableGroup')
parser.add_argument('--host_name', default="localhost", help='Host name/address')
parser.add_argument('--host_port', default="2999", help='Host port')
parser.add_argument('--auth_token', default='AU66mMKxL9fEwHEUe9cMnlK9RTDNLkIfp7vcYdT9cSs=', required=False,
                    help='Valid authentication token.')
parser.add_argument('--source_name', required=True, help='Pass the source name here')
parser.add_argument('--table_group_name', required=True, help='Pass the table group name here')
args = vars(parser.parse_args())


def get_source_id(source_name):
    try:
        source_doc = mongodb[CollectionName.SOURCES].find_one({"name":source_name},{"_id":1})
        return source_doc.get('_id',None)
    except Exception as e:
        print('Error: ' + str(e))
        return None


def get_tablegroup_id(src_id,tablegroup_name):
    try:
        tg_doc = mongodb["table_groups"].find_one({"name":tablegroup_name,"source":src_id},{"_id":1})
        return tg_doc.get('_id',None)
    except Exception as e:
        print('Error: ' + str(e))
        return None


def initiate_ingestion(config, tg_id):
    url = '{protocol}://{ip}:{port}/v1.1/source/table_group/ingest.json?table_group_id={tg_id}&ingestion_type={ing_type}&auth_token={auth_token}'.format(
        ip=config['ip'],
        port=config['port'],
        auth_token=config['auth_token'],
        tg_id=tg_id,
        ing_type="truncate-reload-all",
        protocol=config['protocol'])
    response = IWUtils.ejson_deserialize(requests.post(url).content)
    if response.get('result', {}) is not None:
        logging.info("Initiated Ingestion for tablegroup")
    else:
        logging.error("Ingestion of tablegroup failed: {}".format(response))
        raise SystemExit('Configuration of tablegroup failed')


client_config = {'protocol': 'http', 'ip': args['host_name'], 'port': args['host_port'],
                     'auth_token': args['auth_token']}

src_id = get_source_id(args['source_name'])
if src_id is not None:
    table_group_id = get_tablegroup_id(src_id, args['table_group_name'])
    if table_group_id is not None:
        initiate_ingestion(client_config, str(table_group_id))
