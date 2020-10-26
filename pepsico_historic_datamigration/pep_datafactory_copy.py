import os,csv,subprocess
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
with open("../configs/datafactory_params_new.csv", 'r') as data:
    for line in csv.DictReader(data):
        pipeline_inputs = dict(line)
        input_path = pipeline_inputs["input_path"].lstrip('/')
        output_path = pipeline_inputs["output_path"].lstrip('/')
        subscription_id = pipeline_inputs["subscription_id"]
        rg_name = pipeline_inputs["rg_name"]
        df_name = pipeline_inputs["df_name"]
        service_principal_client_id = pipeline_inputs["service_principal_client_id"]
        service_principal_secret_key = pipeline_inputs["service_principal_secret_key"]
        service_principal_tenant = pipeline_inputs["service_principal_tenant"]
        adlsgen2_url = pipeline_inputs["adlsgen2_url"]
        blob_storagestring = pipeline_inputs["blob_storagestring"]
        adlsgen1_url = pipeline_inputs["adlsgen1_url"]
        if blob_storagestring.lower() != "n/a":
            #Copy from Blob Storage to Gen2
            cmd = "python pep_copyfrom_gen1_to_gen2.py --input_path '{}' --output_path '{}' --source_type blob " \
                  "--subscription_id '{}' --rg_name '{}' --df_name '{}' --service_principal_client_id '{}' " \
                  "--service_principal_secret_key '{}' --service_principal_tenant '{}' --adlsgen2_url '{}' " \
                  "--blob_storagestring '{}' ".format(input_path, output_path, subscription_id, rg_name, df_name,
                                                    service_principal_client_id, service_principal_secret_key,
                                                    service_principal_tenant, adlsgen2_url, blob_storagestring)
        else:
            # Copy from Gen1 to Gen2
            cmd = "python pep_copyfrom_gen1_to_gen2.py --input_path '{}' --output_path '{}' --source_type adls " \
                  "--subscription_id '{}' --rg_name '{}' --df_name '{}' --service_principal_client_id '{}' " \
                  "--service_principal_secret_key '{}' --service_principal_tenant '{}' --adlsgen2_url '{}' " \
                  "--data_lake_store_uri '{}' ".format(input_path, output_path, subscription_id, rg_name, df_name,
                                                    service_principal_client_id, service_principal_secret_key,
                                                    service_principal_tenant, adlsgen2_url, adlsgen1_url)

        print(cmd)
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print('Output if any: ' + out.decode('utf-8'))
        print('Error if any: ' + err.decode('utf-8'))
