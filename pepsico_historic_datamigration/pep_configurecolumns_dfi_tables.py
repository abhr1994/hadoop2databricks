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
import copy,re
import traceback


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

def get_default_templates():
    template = "templates/dfi_template.json"
    with open(template, 'r') as template_file:
        template_obj_str = template_file.read()
        template_obj = IWUtils.ejson_deserialize(template_obj_str)

    # >>> template_obj.keys()
    # dict_keys(['entity', 'tables', 'table_groups', 'entity_configs', 'iw_mappings', 'export'])
    table_template = "templates/dfi_table_template.json"
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


def partition_strategy(table,column_mapping):
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

    return partition

def configure_table(configuration_obj,tabletemplate_obj,hive_schema):
    out = []
    for table in configuration_obj["tables"]:
        try:
            list_of_columns = []
            natural_keys = table['configuration']["ingestion_configuration"].get("natural_key", None)
            if natural_keys:
                list_of_columns = list_of_columns + natural_keys

            list_of_columns = list_of_columns+ [table['configuration']['ingestion_configuration'].get('timestamp_column_update', None)] + \
                              [table['configuration']['ingestion_configuration'].get('batch_id_cdc_column', None)]

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
            table_temp["configuration"]["table"] = table["configuration"]["table"].rstrip("_STG")
            table_temp["configuration"]["configuration"]["target_table_name"] = \
            table['configuration']["ingestion_configuration"]["hive_table_name"].rstrip("_STG")
            table_temp["configuration"]["configuration"]["target_schema_name"] = hive_schema
            try:
                table_temp["configuration"]["configuration"]["natural_keys"] = [column_mapping[col_key] for col_key in
                                                                   table['configuration'][
                                                                       "ingestion_configuration"]["natural_keys".strip('s')]]
            except:
                del table_temp["configuration"]["configuration"]["natural_keys"]

            for key in ['source_relative_path', 'ingest_subdirectories', 'source_file_properties',
                        'exclude_filename_regex', 'include_filename_regex', 'target_relative_path']:
                del table_temp['configuration']['configuration'][key]
            ###partition strategy
            partition = partition_strategy(table, column_mapping)
            table_temp["configuration"]["configuration"]["partition_keys"] = partition

            ##Take care of columns
            table_temp["configuration"]["columns"] = []
            column_json = {"is_mixed_element_text_content": False, "sqlType": 12, "isDeleted": False,
                                "name": "", "json_schema_node_key": 0, "origName": "", "targetSqlType": 12,
                                "is_recursive_data_text_content": False, "isAuditColumn": False}

            for column in table["configuration"]["columns"]:
                column_json_temp = copy.deepcopy(column_json)
                if not column["name"].startswith("ZIW"):
                    for key in ["name","origName","sqlType"]:
                        column_json_temp[key] = column[key]
                    column_json_temp["targetSqlType"] = column_json_temp["sqlType"]
                    if column["type"] in ["timestamp","date"]:
                        column_json_temp["format"] = column["format"]
                    if column["type"] == "decimal":
                        column_json_temp["targetPrecision"] = column["precision"]
                        column_json_temp["targetScale"] = column["scale"]
                        column_json_temp["isUserInputNeeded"] = False
                    table_temp["configuration"]["columns"].append(column_json_temp)


            ziw_columns = [
                {"is_mixed_element_text_content": False, "sqlType": 12, "isDeleted": False, "name": "ziw_file_name",
                 "json_schema_node_key": 0, "origName": "ziw_file_name", "targetSqlType": 12,
                 "is_recursive_data_text_content": False, "isAuditColumn": True},
                {"is_mixed_element_text_content": False, "sqlType": 93, "isDeleted": False,
                 "name": "ziw_file_modified_timestamp", "format": "yyyy-MM-dd HH:mm:ss", "json_schema_node_key": 0,
                 "origName": "ziw_file_modified_timestamp", "targetSqlType": 93,
                 "is_recursive_data_text_content": False, "isAuditColumn": True},
                {"is_mixed_element_text_content": False, "precision": 0, "format": "yyyy-MM-dd HH:mm:ss",
                 "json_schema_node_key": 0, "scale": 6, "origName": "ziw_target_timestamp", "sqlType": 93,
                 "isDeleted": False, "targetScale": "6", "name": "ziw_target_timestamp", "targetPrecision": "0",
                 "targetSqlType": 93, "is_recursive_data_text_content": False, "isAuditColumn": True},
                {"is_mixed_element_text_content": False, "precision": 0, "json_schema_node_key": 0, "scale": 0,
                 "origName": "ziw_is_deleted", "sqlType": 16, "isDeleted": False, "targetScale": "0",
                 "name": "ziw_is_deleted", "targetPrecision": "0", "targetSqlType": 16,
                 "is_recursive_data_text_content": False, "isAuditColumn": True}]
            table_temp["configuration"]["columns"] = table_temp["configuration"]["columns"]+ziw_columns
            out.append(table_temp)
        except Exception as e:
            traceback.print_exc()
            print("Configuration of {} failed {}".format(table["configuration"]["table"],str(e)))

    return out

def convert_csv_onprem_to_db(configuration_file_path):
    with open(configuration_file_path, 'r') as configuration_file:
        configuration = configuration_file.read()
        configuration_obj = IWUtils.ejson_deserialize(configuration)

    #configuration_obj.keys()
    #dict_keys(['entity', 'source', 'tables', 'table_groups', 'entity_configs', 'iw_mappings', 'export'])

    template_obj,tabletemplate_obj,tablegrouptemplate_obj = get_default_templates()
    final = copy.deepcopy(template_obj)

    iw_mapping_temp = []
    for item in configuration_obj["iw_mappings"]:
        if item["entity_type"] == "table":
            item["recommendation"]["table_name"] = item["recommendation"]["table_name"].rstrip("_STG")
            iw_mapping_temp.append(item)
        elif item["entity_type"] == "source":
            iw_mapping_temp.append(item)
        else:
            pass
    configuration_obj["iw_mappings"] = iw_mapping_temp

    for item in ['entity','iw_mappings','export']:
        final[item]=configuration_obj[item]

    logging.info('Preparing to configure the tables. There are {} tables in this source to configure'.format(str(len(configuration_obj["tables"]))))

    hive_schema = configuration_obj["source"]["hive_schema"]
    table_out = configure_table(configuration_obj, tabletemplate_obj, hive_schema)
    for item in table_out:
        final['tables'].append(item)

    return final


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
