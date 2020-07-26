## How to use the scripts in this repository?

```markdown
# Create RDBMS Source

source /opt/infoworks/bin/env.sh;python /home/infoworks/abhi/create_rdbms_source.py --source_connection_file_path /home/infoworks/abhi/source_details.csv --source_creation_template /home/infoworks/abhi/templates/create_source_template.json --host_name localhost --host_port 2999 --auth_token <> --cluster_template default_template

# Add source to cluster template

python add_source_to_clustertemplate.py --source_name Teradata_test --cluster_template default_template

# Script to configure the sources

source /opt/infoworks/bin/env.sh;python /home/infoworks/abhi/source_migration_v2.py --configuration_json_path /home/infoworks/abhi/source_AR_Test_TD.json --source_name AR_Test_TD --source_type rdbms --host_name localhost --host_port 2999 --auth_token <> --cluster_template default_template

# Script to run the historical data migration

source /opt/infoworks/bin/env.sh;python /home/infoworks/abhi/run_migration.py --host <> --token <> --cluster_id 0718-041317-gilt53 --param_file /home/infoworks/abhi/param_file.csv
```

# Requirements

1. The folder structure in ADLS Gen2 should match the target hdfs directory structure of the on-prem sources
2. Source schema and table name should be same in HDI and Databricks

## Supported Features
- Supports migration of timestamp-based incremental tables and batch_id based incremental tables
- Supports reading of parquet and orc files
- Partitioning is supported
- Column renames are supported

## Unsupported Features
- Datatypes over-rides are not supported
