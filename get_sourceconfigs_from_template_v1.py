#!/usr/bin/env python
__author__ = "Abhishek Raviprasad"
__version__ = "0.1.0"
__maintainer__ = "Abhishek Raviprasad"
__email__ = "abhishek.raviprasad@infoworks.io"
__status__ = "Dev"

import os,sys
import logging

logging.getLogger().setLevel(logging.INFO)
try:
    iw_home = os.environ['IW_HOME']
except KeyError as e:
    print('Please source $IW_HOME/bin/env.sh before running this script')
    sys.exit(1)

infoworks_python_dir = os.path.abspath(os.path.join(iw_home, 'apricot-meteor', 'infoworks_python'))
sys.path.insert(0, infoworks_python_dir)

from infoworks.core.iw_utils import IWUtils
from infoworks.core.mongo_utils import mongodb
import copy,re

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

def get_default_templates(source_type):
    if source_type == "rdbms":
        template = "templates/template.json"
    elif source_type == "sfdc":
        template = "templates/sfdc_template.json"
    else:
        template=""
    with open(template, 'r') as template_file:
        template_obj_str = template_file.read()
        template_obj = IWUtils.ejson_deserialize(template_obj_str)

    # >>> template_obj.keys()
    # dict_keys(['entity', 'source', 'tables', 'table_groups', 'entity_configs', 'iw_mappings', 'export'])
    table_template = "templates/table_template.json"
    with open(table_template, 'r') as table_template_file:
        tabletemplate_obj_str = table_template_file.read()
        tabletemplate_obj = IWUtils.ejson_deserialize(tabletemplate_obj_str)

    # >>> tabletemplate_obj.keys()
    # dict_keys(['entity_type', 'entity_id', 'configuration'])
    tablegroup_template = "templates/tablegroup_template.json"
    with open(tablegroup_template, 'r') as tablegroup_template_file:
        tablegrouptemplate_obj_str = tablegroup_template_file.read()
        tablegrouptemplate_obj = IWUtils.ejson_deserialize(tablegrouptemplate_obj_str)

    # >>> tablegrouptemplate_obj.keys()
    # dict_keys(['entity_type', 'entity_id', 'configuration'])

    return (template_obj,tabletemplate_obj,tablegrouptemplate_obj)


def get_db_column_name(columns,names):
    output_names = {}
    for i in range(len(columns)):
        if columns[i]['name'] in names:
            orig = columns[i]['origName']
            if any(not c.isalnum() and c != "_" for c in orig):
                output_names[columns[i]['name']] = re.sub('[^A-Za-z0-9_]+', '', orig)+str(i+1)
            else:
                output_names[columns[i]['name']] = columns[i]['name']

    return output_names


def partition_split_strategy(table,column_mapping):
    mappings = {"day num in month": "dd", "month": "MM", "year": "yyyy", "year month": "yyyyMM", "month day": "MMdd",
     "year month day": "yyyyMMdd"}
    partition = []
    arr = table['configuration']["ingestion_configuration"].get("partition_key", [])
    if arr is not None:
        for element in arr:
            if element['partition_key_extract_from_col'] == True:
                der_func = element['partition_key_derive_function']
                if der_func !="regex":
                    der_func_value = " ".join(der_func.split("-"))
                    partition_obj =  {
                        "parent_column": column_mapping[element['partition_key_column']],
                        "derive_function": der_func_value.strip(),
                        "derive_format": mappings[der_func_value.strip()],
                        "is_derived_partition": True,
                        "partition_column": element['partition_key_derive_column']
                    }
                    partition.append(partition_obj)
            else:
                partition.append({'is_derived_partition': element['partition_key_extract_from_col'],'partition_column': column_mapping[element['partition_key_column']]})


    if table['configuration']['ingestion_configuration'].get('split_by_key_extract_from_col',None):
        der_func = table['configuration']['ingestion_configuration']['split_by_key_derive_function']
        der_func_value = " ".join(der_func.split("-"))
        split_key={
            "parent_column": column_mapping[table['configuration']['ingestion_configuration']['split_by_key']],
            "is_derived_split": True,
            "derive_function": der_func_value,
            "derive_format": mappings[der_func_value.strip()],
            "split_column": "iwDerivedColumn"
        }
    else:
        if table['configuration']['ingestion_configuration'].get('split_by_key', None):
            split_column_name = column_mapping[table['configuration']['ingestion_configuration']['split_by_key']]
        else:
            split_column_name = None
        split_key = {'is_derived_split': False, 'split_column':split_column_name}

    return (partition,split_key)

def configure_table_group(configuration_obj,tablegrouptemplate_obj,cluster_id,source_type):
    out = []
    for i in configuration_obj["table_groups"]:
        tablegrouptemp = copy.deepcopy(tablegrouptemplate_obj)
        tablegrouptemp['entity_type'] = i["entity_type"]
        tablegrouptemp['entity_id'] = i['entity_id']
        for key in tablegrouptemp["configuration"].keys():
            try:
                if key == "tables":
                    connection_quota = round(100/i["configuration"]["max_parallel_entities"])
                    for table in i["configuration"][key]:
                        if not table.get("connection_quota",None):
                            table["connection_quota"] = connection_quota
                        tablegrouptemp["configuration"][key].append(table)
                else:
                    tablegrouptemp["configuration"][key] = i["configuration"][key]
            except:
                pass
        tablegrouptemp["configuration"]["cluster_template"]["cluster_id"] = cluster_id
        if source_type == "sfdc":
            #del (tablegrouptemp["configuration"]["max_connections"])
            tablegrouptemp["configuration"]["job_name"] = tablegrouptemp["configuration"]["name"]
            tablegrouptemp["configuration"]["isTableGroupJob"] = True
            #for i in range(len(tablegrouptemp["configuration"]["tables"])):
            #    del(tablegrouptemp["configuration"]["tables"][i]['connection_quota'])
        out.append(tablegrouptemp)
    return out

def configure_table(configuration_obj,tabletemplate_obj,hive_schema,source_type,source_subtype = None,connection_method = None):
    out = []
    list_of_columns = []
    for table in configuration_obj["tables"]:
        natural_keys = table['configuration']["ingestion_configuration"].get("natural_key", None)
        if natural_keys:
            list_of_columns = list_of_columns + natural_keys

        crawl_filter_conditions = table['configuration']["ingestion_configuration"].get("crawl_filter_conditions", None)
        if crawl_filter_conditions:
            list_of_columns = list_of_columns + [item["filter_column"] for item in table['configuration']["ingestion_configuration"]["crawl_filter_conditions"]]
        list_of_columns = list_of_columns+ [table['configuration']['ingestion_configuration'].get('timestamp_column_update', None)] + \
                          [table['configuration']['ingestion_configuration'].get('batch_id_cdc_column', None)] + \
                          [table['configuration']['ingestion_configuration'].get('split_by_key', None)]

        for item in table['configuration']["ingestion_configuration"].get("partition_key", []):
            list_of_columns.append(item["partition_key_column"])
        list_of_columns = list(filter(None.__ne__, list_of_columns))
        column_mapping = get_db_column_name(table['configuration']['columns'],list_of_columns)

        table_temp = copy.deepcopy(tabletemplate_obj)

        if table['configuration']["ingestion_configuration"]['sync_type'] == "full-load":
            table_temp["configuration"]["configuration"]['sync_type'] = 'full-load'
        elif table['configuration']["ingestion_configuration"]['sync_type'] in ["cdc-timestamp-column", "cdc-batch-id"]:
            table_temp["configuration"]["configuration"]['sync_type'] = 'incremental'
            table_temp["configuration"]["configuration"]['update_strategy'] = "merge"
            if table['configuration']['ingestion_configuration'].get('timestamp_column_update', None):
                table_temp["configuration"]["configuration"]['watermark_column'] = column_mapping[table['configuration']['ingestion_configuration']['timestamp_column_update']]
            if table['configuration']['ingestion_configuration'].get('batch_id_cdc_column', None):
                table_temp["configuration"]["configuration"]['watermark_column'] = column_mapping[table['configuration']['ingestion_configuration']['batch_id_cdc_column']]
        else:
            logging.warning(
                "Synctype not supported. Hence not configuring the table: {}".format(table["configuration"]["table"]))
            continue
        table_temp['entity_type'] = table["entity_type"]
        table_temp['entity_id'] = table['entity_id']
        table_temp["configuration"]["table"] = table["configuration"]["table"]
        table_temp["configuration"]["configuration"]["target_table_name"] = \
        table['configuration']["ingestion_configuration"]["hive_table_name"]
        table_temp["configuration"]["configuration"]["target_schema_name"] = hive_schema
        table_temp["configuration"]["configuration"]['exclude_legacy_audit_columns'] = True
        for j in ['read_supported_engines', 'write_supported_engines']:
            table_temp["configuration"]["configuration"][j] = ["SPARK"]
        for k in ['is_scd2_table', 'is_archive_enabled', 'natural_keys']:
            try:
                if k == "natural_keys":
                    table_temp["configuration"]["configuration"][k] = [column_mapping[col_key] for col_key in
                                                                       table['configuration'][
                                                                           "ingestion_configuration"][k.strip('s')]]
                else:
                    table_temp["configuration"]["configuration"][k] = table['configuration']["ingestion_configuration"][
                        k]
            except:
                pass

        # New in 4.2
        if source_subtype in ["sqlserver", "oracle", "teradata","postgresql","db2_luw"]:
            if table['configuration']["ingestion_configuration"].get("crawl_data_filter_enabled", False):
                table_temp["configuration"]["configuration"]['crawl_data_filter_enabled'] = True
                filter_cond_temp = []
                for item in table['configuration']["ingestion_configuration"]["crawl_filter_conditions"]:
                    item["filter_column"] = column_mapping[item["filter_column"]]
                    filter_cond_temp.append(item)
                table_temp["configuration"]["configuration"]['crawl_filter_conditions'] = filter_cond_temp
            else:
                table_temp["configuration"]["configuration"]['crawl_filter_conditions'] = []
                table_temp["configuration"]["configuration"]['crawl_data_filter_enabled'] = False

        if source_type == "sfdc":
            # Add enable_pk_chunk,fetch_mechanism,watermark_column
            table_temp["configuration"]["configuration"]["watermark_column"] = "SystemModstamp"
            table_temp["configuration"]["configuration"]["fetch_mechanism"] = \
            table['configuration']["ingestion_configuration"]["fetch_mechanism"]
            table_temp["configuration"]["configuration"]["enable_pk_chunk"] = \
            table['configuration']["ingestion_configuration"]["pk_chunking_enabled"]

        ###partition and split strategy
        partition, split = partition_split_strategy(table, column_mapping)
        table_temp["configuration"]["configuration"]["partition_keys"] = partition
        if split['split_column']:
            table_temp["configuration"]["configuration"]['split_by_key'] = split

        if source_subtype == "teradata":
            if connection_method == "tpt":
                table_temp["configuration"]["configuration"]["use_jdbc"] = False
                table_temp["configuration"]["configuration"]["tpt_reader_instances"] = 1
                table_temp["configuration"]["configuration"]["tpt_writer_instances"] = 5
                del (table_temp["configuration"]["configuration"]['split_by_key'])
            else:
                table_temp["configuration"]["configuration"]["use_jdbc"] = False

        if source_subtype == "sqlserver":
            table_temp["configuration"]["configuration"]['use_capture_table'] = False
        out.append(table_temp)

    return out

def convert_rdbms_onprem_to_db(configuration_file_path,cluster_template,source_type,src_id):
    #configuration_file_path="/home/abhi/amn_migration/hadoop_source.json"
    with open(configuration_file_path, 'r') as configuration_file:
        configuration = configuration_file.read()
        configuration_obj = IWUtils.ejson_deserialize(configuration)

    #configuration_obj.keys()
    #dict_keys(['entity', 'source', 'tables', 'table_groups', 'entity_configs', 'iw_mappings', 'export'])

    template_obj,tabletemplate_obj,tablegrouptemplate_obj = get_default_templates(source_type)
    final = copy.deepcopy(template_obj)

    for item in ['entity','iw_mappings', 'export']:
        final[item]=configuration_obj[item]

    for item in final["source"].keys():
        if item == "connection":
            for key in final["source"]["connection"].keys():
                final["source"]["connection"][key] = configuration_obj["source"]["connection"][key]
        else:
            if item == "target_schema_name":
                final["source"][item] = configuration_obj["source"]["hive_schema"]
            else:
                final["source"][item] = configuration_obj["source"][item]

    cluster_out = mongodb["databricks_cluster_templates"].find_one({"cluster_name": cluster_template}, {"_id": 1})
    cluster_id = cluster_out['_id']

    out = configure_table_group(configuration_obj,tablegrouptemplate_obj,cluster_id,source_type)
    for item in out:
        final['table_groups'].append(item)

    logging.info('Preparing to configure the tables. There are {} tables in this source to configure'.format(str(len(configuration_obj["tables"]))))

    hive_schema = final["source"]["target_schema_name"]
    table_out = configure_table(configuration_obj, tabletemplate_obj, hive_schema,source_type,configuration_obj["source"]["sourceSubtype"],configuration_obj["source"]["connection"]["connection_method"])
    for item in table_out:
        final['tables'].append(item)

    return final

def convert_sfdc_onprem_to_db(configuration_file_path,cluster_template,source_type):

    with open(configuration_file_path, 'r') as configuration_file:
        configuration = configuration_file.read()
        configuration_obj = IWUtils.ejson_deserialize(configuration)

    #configuration_obj.keys()
    #dict_keys(['entity', 'source', 'tables', 'table_groups', 'entity_configs', 'iw_mappings', 'export'])

    template_obj,tabletemplate_obj,tablegrouptemplate_obj = get_default_templates(source_type)

    final = copy.deepcopy(template_obj)
    for item in ['entity','iw_mappings', 'export']:
        final[item]=configuration_obj[item]

    for item in final["source"].keys():
        if item == "connection":
            for key in final["source"]["connection"].keys():
                final["source"]["connection"][key] = configuration_obj["source"]["connection"][key]
        else:
            final["source"][item] = configuration_obj["source"][item]

    cluster_out = mongodb["databricks_cluster_templates"].find_one({"cluster_name": cluster_template}, {"_id": 1})
    cluster_id = cluster_out['_id']

    out = configure_table_group(configuration_obj,tablegrouptemplate_obj,cluster_id,source_type)
    for item in out:
        final['table_groups'].append(item)

    logging.info('Preparing to configure the tables. There are {} tables in this source to configure'.format(str(len(configuration_obj["tables"]))))

    hive_schema = final["source"]["target_schema_name"]
    table_out = configure_table(configuration_obj, tabletemplate_obj, hive_schema,source_type)
    for item in table_out:
        final['tables'].append(item)

    return final