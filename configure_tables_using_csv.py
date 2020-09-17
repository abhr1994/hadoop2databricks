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

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

logging.getLogger().setLevel(logging.INFO)
parser = argparse.ArgumentParser('Configure Tables')
parser.add_argument('--host_name', default="localhost", help='Host name/address')
parser.add_argument('--host_port', default="2999", help='Host port')
parser.add_argument('--auth_token', default='AU66mMKxL9fEwHEUe9cMnlK9RTDNLkIfp7vcYdT9cSs=', required=False,
                    help='Valid authentication token.')
parser.add_argument('--configuration_file_path', required=True, help='CSV File path with table details')

args = vars(parser.parse_args())

if not args['configuration_file_path']:
    logging.error('configuration_file_path is required. Exiting')
    sys.exit(-1)

def get_table_config_url(config, entity_id, entity_type):
    url = '{protocol}://{ip}:{port}/v1.1/entity/configure.json?auth_token={auth_token}&entity_id={entity_id}&entity_type={entity_type}'.format(
        ip=config['ip'],
        port=config['port'],
        auth_token=config['auth_token'],
        entity_id=entity_id,
        entity_type=entity_type,
        protocol=config['protocol'])
    return url


def configure_table(table_obj, entity_id):
    client_config = {'protocol': 'http', 'ip': args['host_name'], 'port': args['host_port'],
                     'auth_token': args['auth_token']}
    table_url = get_table_config_url(client_config, entity_id, "table")
    configuration = IWUtils.ejson_serialize(table_obj)
    response = IWUtils.ejson_deserialize(requests.post(table_url, data=configuration).content)
    logging.info('Result of table configuration {}:  {} '.format(
        table_obj["configuration"]["configuration"]["target_table_name"], response))


def get_table_id(src_name, table_name):
    src_obj = mongodb["sources"].find_one({"name": src_name}, {"_id": 1})
    if src_obj:
        table_doc = mongodb["tables"].find_one({"table": table_name, "source": src_obj['_id']}, {"_id": 1})
        if table_doc:
            return str(table_doc['_id'])
        else:
            logging.error("Table: {} not found under Source".format(table_name))
            raise SystemExit('Configuration of Source failed')
    else:
        logging.error("Source details for source name: {} not found in MongoDb".format(src_name))
        raise SystemExit('Configuration of Source failed')


def main(file_path):
    with open("templates/table_config_template.json", 'r') as configuration_file:
        configuration = configuration_file.read()
        table_configuration_obj = IWUtils.ejson_deserialize(configuration)

    with open(file_path, 'r') as data:
        for line in csv.DictReader(data):
            table_to_configure = dict(line)
            table_confobj_copy = copy.deepcopy(table_configuration_obj)
            keys_to_remove = []
            keys_in_csv = [i.lower() for i in table_to_configure.keys()]
            for key in table_confobj_copy["configuration"]["configuration"]:
                if key in keys_in_csv and table_to_configure[key] != "" and table_to_configure[key].lower() != "n/a":
                    if key == "split_by_key":
                        split_by_key = table_to_configure["split_by_key"]
                        table_confobj_copy["configuration"]["configuration"][key] = {'is_derived_split': False, 'split_column': split_by_key}
                    elif key == "partition_keys":
                        partition_keys = []
                        part_keys = table_to_configure["partition_keys"].split(',')
                        for item in part_keys:
                            partition_keys.append({'is_derived_partition': False, 'partition_column': item})
                        table_confobj_copy["configuration"]["configuration"][key] = partition_keys
                    elif key == "natural_keys":
                        natural_keys = []
                        for item in table_to_configure["natural_keys"].split(','):
                            natural_keys.append(item)
                        table_confobj_copy["configuration"]["configuration"][key] = natural_keys
                    else:
                        if table_to_configure[key].lower() == "false":
                            table_confobj_copy["configuration"]["configuration"][key] = False
                        elif table_to_configure[key].lower() == "true":
                            table_confobj_copy["configuration"]["configuration"][key] = True
                        else:
                            table_confobj_copy["configuration"]["configuration"][key] = table_to_configure[key]
                else:
                    keys_to_remove.append(key)
            if table_to_configure["sync_type"] == "full-load" and "update_strategy" not in keys_to_remove:
                keys_to_remove.append("update_strategy")
            for i in keys_to_remove:
                if i not in ["natural_keys","partition_keys","crawl_filter_conditions","crawl_data_filter_enabled"]:
                    del (table_confobj_copy["configuration"]["configuration"][i])

            table_confobj_copy["configuration"]["table"] = table_confobj_copy["configuration"]["configuration"]["target_table_name"]
            src_name = table_to_configure["source_name"]
            table_id = get_table_id(src_name, table_confobj_copy["configuration"]["configuration"]["target_table_name"])
            if table_id:
                configure_table(table_confobj_copy, table_id)
            else:
                logging.warning("Skipping configuration of table {}".format(
                    table_confobj_copy["configuration"]["configuration"]["target_table_name"]))


main(args['configuration_file_path'])
