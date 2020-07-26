#!/usr/bin/env python
__author__ = "Abhishek Raviprasad"
__version__ = "0.1.0"
__maintainer__ = "Abhishek Raviprasad"
__email__ = "abhishek.raviprasad@infoworks.io"
__status__ = "Dev"
__description__ = "Script to add the source to cluster template"

import os,sys,logging,argparse
try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
sys.path.insert(0, infoworks_python_dir)

from infoworks.core.mongo_utils import mongodb

logging.getLogger().setLevel(logging.INFO)
parser = argparse.ArgumentParser('Add Source To Cluster Template')
parser.add_argument('--source_name', required=True, help='Source name to add')
parser.add_argument('--cluster_template', required=True, help='Provide the name of cluster template')
args = vars(parser.parse_args())

if (not args['source_name']) or (not args['cluster_template']):
    logging.error('Mandatory fields missing. Exiting')
    sys.exit(-1)


def update_cluster_template(src_id,cluster_template_name):
    try:
        doc = mongodb["databricks_cluster_templates"].find_one({"cluster_name": cluster_template_name}, {"_id": 1,"accessible_sources":1})
        accessible_sources = set(doc["accessible_sources"])
        accessible_sources.add(src_id)
        mongodb["databricks_cluster_templates"].update({"cluster_name": cluster_template_name},{"$set": {"accessible_sources":list(accessible_sources)}})
        print("Added source to cluster template")
    except Exception as e1:
        print("Unable to update the cluster template {}".format(str(e1)))


def get_src_id(source_name):
    try:
        doc = mongodb["sources"].find_one({"name":source_name},{"_id":1,"name":1})
        src_id = doc['_id']
        return src_id
    except Exception:
        print("Unable to find the Source Id")


def main():
    source_name = args["source_name"]
    cluster_template = args["cluster_template"]
    src_id = get_src_id(source_name)
    print("Got source id: {}".format(str(src_id)))
    if src_id:
        update_cluster_template(src_id,cluster_template)
    else:
        print("Source Doesn't exist!!!")

main()