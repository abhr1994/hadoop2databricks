#!/usr/bin/env python
import os, requests, sys, logging, argparse, csv, base64
try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
sys.path.insert(0, infoworks_python_dir)
from infoworks.core.iw_utils import IWUtils
from infoworks.core.mongo_utils import mongodb

logging.getLogger().setLevel(logging.INFO)
parser = argparse.ArgumentParser('Configure TableGroup')
parser.add_argument('--host_name', default="localhost", help='Host name/address')
parser.add_argument('--host_port', default="2999", help='Host port')
parser.add_argument('--auth_token', default='AU66mMKxL9fEwHEUe9cMnlK9RTDNLkIfp7vcYdT9cSs=', required=False,
                    help='Valid authentication token.')
parser.add_argument('--configuration_file_path', required=True, help='Pass the config file here')
parser.add_argument('--cluster_template', default="job_default", required=False,
                    help='Pass the cluster template name here')
parser.add_argument('--initiate_ingestion', default="n", help='Pass the y/n here')

args = vars(parser.parse_args())


def get_tablegroup_config_url(config, src_id):
    url = '{protocol}://{ip}:{port}/v1.1/source/table_group/create.json?source_id={src_id}&auth_token={auth_token}'.format(
        ip=config['ip'],
        port=config['port'],
        auth_token=config['auth_token'],
        src_id=src_id,
        protocol=config['protocol'])
    return url


def get_tablegroup_config(list_of_tables, cluster_id, tablegroup):
    max_entities = int(tablegroup.get("max_parallel_entities", 1))
    if tablegroup["sourcetype"].lower() == "rdbms":
        configuration_obj = {
            "configuration": {
                "name": tablegroup.get("tablegroup_name", base64.b32encode(str(tablegroup).encode("UTF-8"))),
                "max_parallel_entities": max_entities, "tables": [],
                "cluster_template": {"cluster_id": cluster_id},
                "max_connections": int(tablegroup.get("max_connections", 10))}}
        connection_quota = round(100 / max_entities)
        for item in list_of_tables:
            configuration_obj["configuration"]["tables"].append({"table_id": item, "connection_quota": connection_quota})
    else:
        configuration_obj = {
            "configuration": {
                "name": tablegroup.get("tablegroup_name", base64.b32encode(str(tablegroup).encode("UTF-8"))),
                "max_parallel_entities": max_entities, "tables": [],
                "cluster_template": {"cluster_id": cluster_id}
                }}
        for item in list_of_tables:
            configuration_obj["configuration"]["tables"].append({"table_id": item})
    configuration_serialized = IWUtils.ejson_serialize(configuration_obj)
    return configuration_serialized


def get_source_details(src_name,tables):
    table_ids = []
    src_obj = mongodb["sources"].find_one({"name": src_name}, {"_id": 1})
    if src_obj:
        for table_name in tables.split(','):
            try:
                table_obj = mongodb["tables"].find_one({"source": src_obj['_id'], "table":table_name}, {"_id": 1})
                #import pdb;
                #pdb.set_trace()
                table_ids.append(table_obj.get("_id",None))
            except:
                pass
        table_ids = list(filter(None.__ne__, table_ids))
        return src_obj['_id'], table_ids
    else:
        logging.error("Source details for source name: {} not found in MongoDb".format(src_name))
        return None, None


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


def main(file_path):
    with open(file_path, 'r') as data:
        for line in csv.DictReader(data):
            tablegroup = dict(line)
            source_id, list_of_tables = get_source_details(tablegroup["source_name"].strip(), tablegroup["tables"])
            if (source_id is not None) and (list_of_tables is not None):
                client_config = {'protocol': 'http', 'ip': args['host_name'], 'port': args['host_port'],
                                 'auth_token': args['auth_token']}
                tg_configure_url = get_tablegroup_config_url(client_config, str(source_id))
                logging.info("Table group configuration URL is {}".format(tg_configure_url))
                cluster_id = mongodb["databricks_cluster_templates"].find_one({"cluster_name": args["cluster_template"].strip()},
                                                                              {"_id": 1})
                if cluster_id:
                    configuration = get_tablegroup_config(list_of_tables, cluster_id['_id'],tablegroup)
                    response = IWUtils.ejson_deserialize(requests.post(tg_configure_url, data=configuration).content)
                    if response.get('result', {}) is not None:
                        logging.info('Configuration of tablegroup done. {} '.format(response))
                        table_group_id = str(response['result']['entity_id'])
                        if args['initiate_ingestion'].lower() == "y":
                            initiate_ingestion(client_config, table_group_id)
                    else:
                        logging.error('Configuration of tablegroup failed. {} '.format(response))
                        raise SystemExit('Configuration of tablegroup failed')
                else:
                    logging.error('Configuration of tablegroup failed. {} '.format("Wrong Cluster Name Passed"))
                    raise SystemExit('Configuration of tablegroup failed')
            else:
                logging.error('Configuration of tablegroup failed')
                raise SystemExit('Configuration of tablegroup failed')


main(args['configuration_file_path'])
