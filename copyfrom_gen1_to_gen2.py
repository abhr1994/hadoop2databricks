from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time
import argparse,sys

def print_item(group):
    """Print an Azure object instance."""
    print("\tName: {}".format(group.name))
    print("\tId: {}".format(group.id))
    if hasattr(group, 'location'):
        print("\tLocation: {}".format(group.location))
    if hasattr(group, 'tags'):
        print("\tTags: {}".format(group.tags))
    if hasattr(group, 'properties'):
        print_properties(group.properties)
    print("\n")


def print_properties(props):
    """Print a ResourceGroup properties instance."""
    if props and hasattr(props, 'provisioning_state') and props.provisioning_state:
        print("\tProperties:")
        print("\t\tProvisioning State: {}".format(props.provisioning_state))
    print("\n")


def print_activity_run_details(activity_run):
    """Print activity run details."""
    print("\n\tActivity run details\n")
    print("\tActivity run status: {}".format(activity_run.status))
    if activity_run.status == 'Succeeded':
        print("\tNumber of bytes read: {}".format(
            activity_run.output['dataRead']))
        print("\tNumber of bytes written: {}".format(
            activity_run.output['dataWritten']))
        print("\tCopy duration: {}".format(
            activity_run.output['copyDuration']))
    else:
        print("\tErrors: {}".format(activity_run.error['message']))


def main():
    parser = argparse.ArgumentParser(description='Delete databricks Jobs')
    parser.add_argument('--input_path', dest='input_path', required=True, help="Pass the Input ADLS Gen1/Blob Path here")
    parser.add_argument('--output_path', dest='output_path', required=True, help="Pass the Output ADLS Gen2 Path here")
    parser.add_argument('--subscription_id', dest='subscription_id', required=True,help="Pass the Subscription ID here")
    parser.add_argument('--rg_name', dest='rg_name', required=True,help="Pass the Resource Group Name here")
    parser.add_argument('--df_name', dest='df_name', required=True, help="Pass the Datafactory Name here")
    parser.add_argument('--service_principal_client_id', dest='service_principal_client_id', required=True,
                        help="Pass the ServicePrincipal ClientID here")
    parser.add_argument('--service_principal_secret_key', dest='service_principal_secret_key', required=True,
                        help="Pass the ServicePrincipal Secret Key here")
    parser.add_argument('--service_principal_tenant', dest='service_principal_tenant', required=True,
                        help="Pass the ServicePrincipal TenantID here")
    parser.add_argument('--adlsgen2_storagestring',dest='adlsgen2_storagestring', required=True,help='DefaultEndpointsProtocol=https;AccountName=<>;AccountKey=<>')
    parser.add_argument('--data_lake_store_uri',dest='data_lake_store_uri', required=False,help='htts://<account_name>.azuredatalakestore.net/webhdfs/v1')
    parser.add_argument('--blob_storagestring', dest='blob_storagestring', required=False,
                        help='DefaultEndpointsProtocol=https;AccountName=<>;AccountKey=<>')
    parser.add_argument('--source_type',dest='source_type',required=True,help='ADLS/Blob')

    args = vars(parser.parse_args())

    # Azure subscription ID
    subscription_id = args['subscription_id']
    rg_name = args['rg_name']

    # The data factory name. It must be globally unique.
    df_name = args['df_name']

    # Specify your Active Directory client ID, client secret, and tenant ID
    credentials = ServicePrincipalCredentials(
        client_id=args['service_principal_client_id'], secret=args['service_principal_secret_key'], tenant=args['service_principal_tenant'])
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    # Create an ADLS Gen2 Storage linked service
    ls_name = 'storageLinkedService'
    # Specify the name and key of your Azure Storage account
    #storage_string = SecureString(value='DefaultEndpointsProtocol=https;AccountName=pepsigen2adls;AccountKey=6xAztEmI77tB1+x5PWWPMJ+ovz/hfjFfoVAES6jiM83p2/oRjfdPRx1FHGubmKITF3yut86/JviNwlvlbR7O2w==')
    storage_string = SecureString(value=args['adlsgen2_storagestring'])
    ls_azure_storage = AzureStorageLinkedService(connection_string=storage_string)
    print(rg_name)
    ls = adf_client.linked_services.create_or_update(rg_name, df_name, ls_name, ls_azure_storage)
    print_item(ls)

    if args['source_type'].lower() == "adls":
        # Create a ADL Gen1 Storage linked service
        ls_name_adls='storageLinkedAdlsService'
        ls_azure_adls_storage = AzureDataLakeStoreLinkedService(data_lake_store_uri=args['data_lake_store_uri'],subscription_id=subscription_id,tenant=args['service_principal_tenant'])
        print(ls_azure_adls_storage)
        ls_adls = adf_client.linked_services.create_or_update(rg_name, df_name, ls_name_adls, ls_azure_adls_storage)
        print_item(ls_adls)

    if args['source_type'].lower() == "blob":
        # Create an Blob Storage linked service
        ls_name_blob = 'strorageLinkedBlobService'
        #storage_string_blob = SecureString(value='DefaultEndpointsProtocol=https;AccountName=copygen1togen2blobsource;AccountKey=BSA17/S0rorUEUDvtBka3DP41uiNUEXTmso6ApUPDlTgyymu65G+4Xub9Cgv6x6yujc+T8yLuokzPZoUN+0wSg==')
        storage_string_blob = SecureString(value=args['blob_storagestring'])
        ls_azure_blob_storage = AzureStorageLinkedService(connection_string=storage_string_blob)
        ls_blob = adf_client.linked_services.create_or_update(rg_name, df_name, ls_name_blob, ls_azure_blob_storage)
        print_item(ls_blob)

    # Create an Azure ADL dataset (input)
    ds_name = 'ds_in'
    if args['source_type'].lower() == "adls":
        ds_ls_adls_blob = LinkedServiceReference(reference_name=ls_name_adls)
    elif args['source_type'].lower() == "blob":
        ds_ls_adls_blob = LinkedServiceReference(reference_name=ls_name_blob)
    else:
        sys.exit(1)
    inp_path = args['input_path']
    ds_azure_adlorblob = AzureDataLakeStoreDataset(linked_service_name=ds_ls_adls_blob, folder_path=inp_path,file_name='*')
    ds = adf_client.datasets.create_or_update(rg_name, df_name, ds_name, ds_azure_adlorblob)
    print_item(ds)

    # Create an Azure blob dataset (output)
    dsOut_name = 'ds_out'
    ds_ls = LinkedServiceReference(reference_name=ls_name)
    output_blobpath = args['output_path']
    dsOut_azure_blob = AzureBlobDataset(linked_service_name=ds_ls, folder_path=output_blobpath)
    dsOut = adf_client.datasets.create_or_update(rg_name, df_name, dsOut_name, dsOut_azure_blob)
    print_item(dsOut)

    # Create a copy activity
    act_name = 'copyGen1ToGen2'
    adl_source = AzureDataLakeStoreSource()
    blob_sink = BlobSink(copy_behavior="PreserveHierarchy")
    dsin_ref = DatasetReference(reference_name=ds_name)
    dsOut_ref = DatasetReference(reference_name=dsOut_name)
    copy_activity = CopyActivity(name=act_name, inputs=[dsin_ref], outputs=[dsOut_ref], source=adl_source, sink=blob_sink)


    # Create a pipeline with the copy activity
    p_name = 'copyPipeline'
    params_for_pipeline = {}
    p_obj = PipelineResource(
        activities=[copy_activity], parameters=params_for_pipeline)
    p = adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj)
    print_item(p)
    # Create a pipeline run
    run_response = adf_client.pipelines.create_run(rg_name, df_name, p_name, parameters={})
    # Monitor the pipeline run
    time.sleep(30)
    pipeline_run = adf_client.pipeline_runs.get(
        rg_name, df_name, run_response.run_id)
    print("\n\tPipeline run status: {}".format(pipeline_run.status))
    filter_params = RunFilterParameters(
        last_updated_after=datetime.now() - timedelta(1), last_updated_before=datetime.now() + timedelta(1))
    query_response = adf_client.activity_runs.query_by_pipeline_run(
        rg_name, df_name, pipeline_run.run_id, filter_params)
    print_activity_run_details(query_response.value[0])

# Start the main method
main()
