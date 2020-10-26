"""
#!/bin/bash
tbl_name='tableNames.txt'
ddl_name='ddls_out.txt'
rm -f $ddl_name
wait
cat $tbl_name | while read LINE
   do
   beeline -u "jdbc:hive2://zk0-pepsih.52zf0r0glvzurpv45ocrfgxc3b.gx.internal.cloudapp.net:2181,zk1-pepsih.52zf0r0glvzurpv45ocrfgxc3b.gx.internal.cloudapp.net:2181,zk3-pepsih.52zf0r0glvzurpv45ocrfgxc3b.gx.internal.cloudapp.net:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2?hive.execution.engine=tez;hive.auto.convert.join=true;hive.insert.into.multilevel.dirs=true;hive.mapred.supports.subdirectories=true;mapred.input.dir.recursive=true;hive.exec.parallel=true;hive.merge.mapfiles=true;" --showHeader=false --silent=true --outputformat=dsv -e "show create table $LINE" | sed -z 's/\n/ /g' >> $ddl_name
   echo  -e "\n" >> $ddl_name
   echo $LINE
   done
echo "Table DDL generated"


import re
ddls = open('ddls_out.txt').readlines()
ddls = [ re.sub('\s+',' ',ddl.strip()) for ddl in ddls if ddl.strip()!='']
pattern = "^(CREATE EXTERNAL TABLE) * `(.*?)`"
with open("final_ddl_file.txt",'w') as f:
    for ddl in ddls:
        s2 = ddl.split('TBLPROPERTIES')[0]
        l = s2.split('LOCATION')
        l[-1] = "'" + '/mnt/datafoundry/hdi_sources/' + "/".join(l[-1].strip().strip("'").split('/')[3:]) + "'"
        final_string = " LOCATION ".join(l)
        matchObj = re.match(pattern,final_string)
        table = matchObj.group(2).strip()
        db,tbl = table.split('.')
        final_string = final_string.replace('TABLE `{}`'.format(table), 'TABLE `{}`.`{}`'.format("pep_migration_"+db,tbl))
        f.write(final_string)
        f.write("\n")
"""

import pandas as pd
df = pd.read_csv('table_details.csv')
dbs = list(df['sqoop_src_schema'])
#Get the list of all tables
for db in dbs:
    f = open("/tmp/hditablesdump/hqldump_{}.ddl".format(db), "w")
    tables = spark.catalog.listTables(db)
    for t in tables:
        DDL = spark.sql("SHOW CREATE TABLE {}.{}".format(db, t.name))
        s1 = str(DDL.first()[0].replace("\n", " "))
        s2 = s1.split('TBLPROPERTIES')[0]
        l = s2.split('LOCATION')
        l[-1] = "'" + '/mnt/datafoundry/hdi_sources/' + "/".join(l[-1].strip().strip("'").split('/')[3:]) + "'"
        final_string = " LOCATION ".join(l)
        final_string.replace('TABLE `{}`'.format(db), 'TABLE `{}`'.format("pep_migration_" + db))
        f.write(final_string)
        f.write("\n")
    f.close()


###Copy Data from ADLS Gen1 to /mnt/datafoundry/hdi_sources Gen2#####

###########Run in databricks###########
import os,csv

def create_db(adls_src_schema):
    database_ddl = "CREATE DATABASE IF NOT EXISTS {}".format("pep_migration_" + adls_src_schema)
    spark.sql(database_ddl)

def create_table(hql_file):
    fr = open(hql_file)
    for query in fr:
        try:
            results = spark.sql(query)
            print("Query {} executed succesfully".format(query))
        except Exception as e:
            print(str(e))
    fr.close()

#Create all databases
import pandas as pd
df = pd.read_csv('/dbfs/FileStore/tables/table_details.csv')
dbs = list(set(list(df['adls_src_schema'])))
for db in dbs:
    create_db(db)

#Create all tables
files = os.listdir("/dbfs/mnt/datafoundry/deltadb/scripts/")
for file_name in files:
    create_table("/dbfs/mnt/datafoundry/deltadb/scripts/" + file_name)

#Run repair commands
with open("/dbfs/FileStore/tables/table_details.csv", 'r') as data:
    for line in csv.DictReader(data):
        table_details = dict(line)
        adls_src_schema = "pep_migration_"+table_details["adls_src_schema"]
        adls_table_name = table_details["adls_table_name"]
        from pyspark.sql.utils import AnalysisException
        try:
            spark.sql("MSCK REPAIR TABLE {}.{}".format(adls_src_schema,adls_table_name))
        except AnalysisException:
            print("MSCK Command failed for {}.{}. Table is not partitioned".format(adls_src_schema,adls_table_name))



