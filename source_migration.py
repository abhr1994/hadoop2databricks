#!/usr/bin/env python
import os,requests,sys,logging,argparse
try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
sys.path.insert(0, infoworks_python_dir)

from infoworks.core.iw_utils import IWUtils
from infoworks.sdk.url_builder import get_entity_id_url, get_tables_and_table_groups_configuration_url
from get_sourceconfigs_from_template import convert_rdbms_onprem_to_db,convert_sfdc_onprem_to_db
from infoworks.core.mongo_utils import mongodb
from bson import ObjectId


logging.getLogger().setLevel(logging.INFO)
parser = argparse.ArgumentParser('Migrate Source')
parser.add_argument('--auth_token', default='AU66mMKxL9fEwHEUe9cMnlK9RTDNLkIfp7vcYdT9cSs=', required=False, help='Valid authentication token.')
parser.add_argument('--configuration_json_path', default='', help='Configuration JSON path.')
parser.add_argument('--source_name', required=True, help="Source name to configure.")
parser.add_argument('--source_type', required=True, help="Provide the type of source your want to migrate (rdbms/sfdc)")
parser.add_argument('--host_name', default="localhost", help='Host name/address')
parser.add_argument('--host_port', default="2999", help='Host port')
parser.add_argument('--cluster_template', required=True, default="", help='Provide the name of cluster template')
args = vars(parser.parse_args())

if not args['source_name']:
    logging.error('source_name is required. Exiting')
    sys.exit(-1)
if not args['configuration_json_path']:
    logging.error('configuration_json_path is required. Exiting')
    sys.exit(-1)
if not args['auth_token']:
    logging.warning('auth_token not passed. Switching to default')

client_config = {
        'protocol': 'http',
        'ip': args['host_name'],
        'port': args['host_port'],
        'auth_token': args['auth_token']
    }

def set_cluster_template(cid,src_id):
    try:
        cluster_id = mongodb["databricks_cluster_templates"].find_one({"cluster_name": cid}, {"_id": 1})
        src_id = ObjectId(src_id)
        cid = cluster_id['_id']
        mongodb["table_groups"].update_many({"source": src_id}, {"$set": {"cluster_template": {"cluster_id": cid}}})
        logging.info('Setting the cluster template to all the tablegroups of source done.')
    except:
        logging.info('Setting the cluster template failed.')


url_for_getting_source_id = get_entity_id_url(client_config, args['source_name'], 'source')
logging.info('URL to get the source id is {} '.format(url_for_getting_source_id))
existing_source_id = IWUtils.ejson_deserialize(requests.get(url_for_getting_source_id).content)
existing_source_id_result = existing_source_id.get('result')
if existing_source_id_result and existing_source_id_result.get('entity_id') is not None:
    source_id = str(existing_source_id_result.get('entity_id'))
    logging.info('Got source id  {} '.format(source_id))
else:
    logging.error('Can not find source with given name {} '.format(args['source_name']))
    sys.exit(-1)

configure_source_url = get_tables_and_table_groups_configuration_url(client_config, source_id)
logging.info('URL to configure the source is {} '.format(configure_source_url))

logging.info('Trying to convert the on-prem JSON config file to Databricks compatible')
configuration_file_path=args['configuration_json_path']
if args['source_type'].lower() == "rdbms":
    configuration_obj = convert_rdbms_onprem_to_db(configuration_file_path,args['cluster_template'],args['source_type'].lower(),source_id)
elif args['source_type'].lower() == "sfdc":
    configuration_obj = convert_sfdc_onprem_to_db(configuration_file_path, args['cluster_template'],args['source_type'].lower())
else:
    configuration_obj = None

if configuration_obj:
    logging.info('Conversion of config file successful')
    configuration = IWUtils.ejson_serialize(configuration_obj)
    logging.info('The configuration object to configure the source is {} '.format(configuration))
    response = IWUtils.ejson_deserialize(requests.post(configure_source_url, data=configuration).content)
    logging.info('Configuration of source done {} '.format(response))
    set_cluster_template(args['cluster_template'], source_id)
else:
    logging.error('Conversion of config file failed. Not configuring the source')
