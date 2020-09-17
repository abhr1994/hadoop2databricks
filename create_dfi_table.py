#!/usr/bin/env python
__author__ = "Abhishek Raviprasad"
__version__ = "0.1.0"
__maintainer__ = "Abhishek Raviprasad"
__email__ = "abhishek.raviprasad@infoworks.io"
__status__ = "Dev"

import os,sys
import logging,argparse

logging.getLogger().setLevel(logging.INFO)
try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
sys.path.insert(0, infoworks_python_dir)

from infoworks.core.iw_utils import IWUtils
from infoworks.sdk.url_builder import get_entity_id_url, get_tables_and_table_groups_configuration_url
import copy,requests

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

logging.getLogger().setLevel(logging.INFO)
parser = argparse.ArgumentParser('Migrate Source')
parser.add_argument('--auth_token', default='AU66mMKxL9fEwHEUe9cMnlK9RTDNLkIfp7vcYdT9cSs=', required=False, help='Valid authentication token.')
parser.add_argument('--configuration_json_path', default='', help='Configuration JSON path.')
parser.add_argument('--source_name', required=True, help="Source name to configure.")
parser.add_argument('--host_name', default="localhost", help='Host name/address')
parser.add_argument('--host_port', default="2999", help='Host port')

args = vars(parser.parse_args())

client_config = {
        'protocol': 'http',
        'ip': args['host_name'],
        'port': args['host_port'],
        'auth_token': args['auth_token']
    }


def convert_csv_onprem_to_db(configuration_file_path):
    table_template = 'templates/create_dfi_table.json'
    with open(table_template, 'r') as table_template_file:
        tabletemplate_obj_str = table_template_file.read()
        tabletemplate_obj = IWUtils.ejson_deserialize(tabletemplate_obj_str)

    with open(configuration_file_path, 'r') as configuration_file:
        configuration = configuration_file.read()
        configuration_obj = IWUtils.ejson_deserialize(configuration)

    output_json = {}
    output_tables = []
    for table in configuration_obj['tables']:
        final = copy.deepcopy(tabletemplate_obj)
        for i in ['entity_type', 'entity_id']:
            final[i] = table[i]

        for key in final['configuration']['configuration'].keys():
            if key == "write_supported_engines":
                pass
            elif key == 'target_table_name':
                final['configuration']['configuration'][key] = table['configuration']['configuration']['hive_table_name']
            elif key == 'target_relative_path':
                final['configuration']['configuration'][key] = table['configuration']['configuration']['hdfs_relative_path']
            elif key == 'source_file_properties':
                final['configuration']['configuration'][key] = table['configuration']['configuration'][key]
                del final['configuration']['configuration'][key]['footer_rows_count']
            else:
                final['configuration']['configuration'][key] = table['configuration']['configuration'][key]
        final['configuration']['table'] = table['configuration']['table']
        output_tables.append(final)

    output_json["tables"] = output_tables
    output_json["iw_mappings"] = configuration_obj["iw_mappings"]
    return output_json

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
configuration_obj = convert_csv_onprem_to_db(configuration_file_path)

if configuration_obj:
    logging.info('Conversion of config file successful')
    configuration = IWUtils.ejson_serialize(configuration_obj)
    #logging.info('The configuration object to configure the source is {} '.format(configuration))
    response = IWUtils.ejson_deserialize(requests.post(configure_source_url, data=configuration).content)
    logging.info('Configuration of source done {} '.format(response))
else:
    logging.error('Conversion of config file failed. Not configuring the source')
