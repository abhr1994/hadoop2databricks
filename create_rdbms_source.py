#!/usr/bin/env python
__author__ = "Abhishek Raviprasad"
__version__ = "0.1.0"
__maintainer__ = "Abhishek Raviprasad"
__email__ = "abhishek.raviprasad@infoworks.io"
__status__ = "Dev"
__description__ = "Script to create the RDBMS Sources in Databricks using a csv file with source details"


import logging
import os
import requests
import sys,time

import argparse

try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
sys.path.insert(0, infoworks_python_dir)

from infoworks.core.iw_utils import IWUtils
from infoworks.sdk.url_builder import get_source_creation_url,get_crawl_metadata_url

from infoworks.core.mongo_utils import mongodb
from infoworks.core.iw_constants import CollectionName
from bson import ObjectId

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def add_source_to_clustertemplate(src_name,cluster_template):
    try:
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        os.chdir(dname)
        cmd = "python add_source_to_clustertemplate.py --source_name {} --cluster_template {}".format(src_name,cluster_template)
        out = os.popen(cmd).read()
        print(out)
    except Exception:
        print("Unable to add source to cluster template!!!")

def make_source_ready(source_id):
    try:
        src_id = ObjectId(source_id)
        json_to_upd = {"state":"ready","is_source_ingested" : True}
        mongodb[CollectionName.SOURCES].update_one({'_id': src_id}, {'$set': json_to_upd})
    except Exception as e:
        print("Error while switching source to ready:  {}".format(str(e)))

def crawl_metadata(host,port,auth_token,source_id):
    #POST /v1.1/source/crawl_metadata.json
    config = {"ip":host,"port":port,"protocol":"http","auth_token":auth_token}
    metadata_crawl_url = get_crawl_metadata_url(config, source_id)
    logging.info(bcolors.OKGREEN+'Metadata Crawl url is {} '.format(metadata_crawl_url)+bcolors.ENDC)
    try:
        response = IWUtils.ejson_deserialize(requests.post(metadata_crawl_url).content)
        result = response.get('result', {})
        if result is not None:
            logging.info(bcolors.OKGREEN+'Metacrawl initiated succesfully for {}.'.format(source_id)+bcolors.ENDC)
        else:
            logging.error(bcolors.FAIL+'Metacrawl failed.{}'.format(response)+bcolors.ENDC)
    except Exception as ex:
        logging.exception(bcolors.FAIL+'Error Occured {}'.format(ex)+bcolors.ENDC)

def main():
    sources_created = []
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser('Create Source')
    parser.add_argument('--auth_token', default='AU66mMKxL9fEwHEUe9cMnlK9RTDNLkIfp7vcYdT9cSs=', required=False, help='Valid authentication token.')
    parser.add_argument('--source_connection_file_path',required=True, default='', help='Source Connection file path')
    parser.add_argument('--source_creation_template',required=True, default='', help='Source Creation Template')
    parser.add_argument('--host_name', default='localhost',required=False, help='Host name/address')
    parser.add_argument('--host_port', default='2999',required=False, help='Host port')
    parser.add_argument('--cluster_template', default='default_template',required=False, help='Pass the cluster template name ')

    args = vars(parser.parse_args())

    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    if not args['source_connection_file_path']:
        logging.error(bcolors.FAIL+'source_connection_file_path is required. Exiting'+bcolors.ENDC)
        sys.exit(-1)
    if not args['source_creation_template']:
        logging.error(bcolors.FAIL+'source_creation_template is required. Exiting'+bcolors.ENDC)
        sys.exit(-1)

    if not args['auth_token']:
        logging.warning(bcolors.WARNING+'auth_token not passed. Switching to default'+bcolors.ENDC)

    if os.path.exists(args['source_connection_file_path']):
        source_list = open(args['source_connection_file_path']).readlines()
        header = source_list[0].split('|')
        for source in source_list[1:]:
            source_row = source.split('|')
            created_source_id = CreateSource().create_source(args['auth_token'],args['source_creation_template'], args['host_name'], args['host_port'],header,source_row,args['cluster_template'])
            logging.info(bcolors.OKGREEN+'Created Source Id.{} '.format(created_source_id)+bcolors.ENDC)
            sources_created.append(created_source_id)

        logging.info(bcolors.HEADER+'Starting the crawl metadata job for all the sources created'+bcolors.ENDC)
        for source in sources_created:
            crawl_metadata(args['host_name'], args['host_port'], args['auth_token'], source)
            make_source_ready(source)
            time.sleep(3)
    else:
        logging.error(bcolors.FAIL+"source_connection_file_path doesn't exist"+bcolors.ENDC)
        sys.exit(-1)


def update_teradata_tpt_details(src_id,tpt_dict,td_wallet):
    print("Updating the teradata tpt source ".format(src_id))
    try:
        src_id = ObjectId(src_id)
        if len(td_wallet) > 0:
            print(tpt_dict,td_wallet)
            json_to_upd = {"connection.intermediate_storage":tpt_dict,"connection.is_td_wallet_enabled":True,"connection.td_wallet_key_used_for_password":td_wallet["td_wallet_key_used_for_password"]}
        else:
            print(tpt_dict)
            json_to_upd = {"connection.intermediate_storage": tpt_dict,"connection.is_td_wallet_enabled":False}
        mongodb["sources"].update_one({'_id': src_id}, {'$set': json_to_upd})
    except Exception as e:
        print("Error while updating TPT details : {}".format(str(e)))

class CreateSource:
    def __init__(self):
        pass
    def create_source(self, auth_token,source_creation_template, host_name,host_port,header,source_details,cluster_template):
        client_config = {
            'protocol': 'http',
            'ip': host_name,
            'port': host_port,
            'auth_token': auth_token
        }
        if os.path.exists(source_creation_template):
            with open(source_creation_template, 'r') as connection_file:
                connection = connection_file.read()
                connection_object = IWUtils.ejson_deserialize(connection)

        temp_dict = dict(zip(header, source_details))

        for item in connection_object["configuration"].keys():
            if item == "connection":
                for j in connection_object["configuration"]['connection'].keys():
                    if j == "password":
                        p = os.popen("bash " + infoworks_python_dir + "/infoworks/bin/infoworks_security.sh -decrypt -p " +temp_dict["connection."+j])
                        connection_object["configuration"]["connection"][j] = p.read().strip()
                    else:
                        if temp_dict.get("connection."+j,None):
                            connection_object["configuration"]["connection"][j] = temp_dict["connection."+j]
            else:
                try:
                    if item == "target_base_path":
                        connection_object["configuration"][item] = os.path.join("/mnt/datafoundry",temp_dict["hdfs_path"].lstrip('/'))
                    elif item == "target_schema_name":
                        connection_object["configuration"][item] = temp_dict["hive_schema"]
                    else:
                        connection_object["configuration"][item] = temp_dict[item]
                except:
                    pass

        if temp_dict["sourceSubtype"] == "teradata":
            if temp_dict["connection.connection_method"] == "jdbc":
                connection_object["configuration"]["connection"]["enable_tpt_for_source"] = False
            else:
                connection_object["configuration"]["connection"]["enable_tpt_for_source"] = True
            connection_object["configuration"]["connection"]["connection_method"] = "jdbc"

        if temp_dict["sourceSubtype"] == "oracle":
            connection_object["configuration"]["configuration"] = {}
            connection_object["configuration"]["configuration"]["oracle_logbased_cdc_enabled"] = False

        connection = IWUtils.ejson_serialize(connection_object)
        print(connection)
        logging.info(bcolors.HEADER+'Getting source creation url.'+bcolors.ENDC)
        source_creation_url = get_source_creation_url(client_config, auth_token)
        logging.info(bcolors.OKGREEN+'Source Creation url is {} '.format(source_creation_url)+bcolors.ENDC)
        if connection is not None:
            try:
                response = IWUtils.ejson_deserialize(requests.post(source_creation_url, data=connection).content)
                result = response.get('result', {})
                if result is not None:
                    logging.info(bcolors.OKGREEN+'Source successfully created.'+bcolors.ENDC)
                    src_name = temp_dict["name"]
                    add_source_to_clustertemplate(src_name, cluster_template)
                    source_id = result.get('entity_id', None)
                    if temp_dict["sourceSubtype"] == "teradata" and temp_dict["connection.connection_method"] == "tpt":
                        tpt_details = open('templates/tpt_details.csv').readlines()
                        tpt_dict = dict(zip(tpt_details[0].strip().split(',')[:-1], tpt_details[1].strip().split(',')[:-1]))
                        td_wallet = {}
                        if tpt_details[1].strip().split(',')[-1] != '':
                            td_wallet["td_wallet_key_used_for_password"] = tpt_details[1].strip().split(',')[-1]
                        update_teradata_tpt_details(source_id, tpt_dict, td_wallet)
                    return source_id
                else:
                     logging.error(bcolors.FAIL+'Source Can not be created.{}'.format(response)+bcolors.ENDC)
            except Exception as ex:
                logging.exception(bcolors.FAIL+'Error Occured {}'.format(ex)+bcolors.ENDC)
                logging.error(bcolors.FAIL+'Can not create source {} '.format(response)+bcolors.ENDC)

if __name__ == '__main__':
    main()