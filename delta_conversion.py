import sys,os,json
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import explode,col
import quinn

#Linking with Spark Log4j
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.setLevel(log4jLogger.Level.DEBUG)


def rename_col(s):
    if override_dict.get(s):
        return override_dict[s]
    else:
        return s

def set_spark_confs():
    confs = open("/dbfs/FileStore/tables/confs/spark_defaults.conf").readlines()
    for conf in confs:
        key,value = conf.split()
        spark.conf.set(key.strip(),value.strip())

def convert_to_delta(src_dir,delta_dir,mode,source_schema,table_name,src_type,format,partition_cols,override_columns,repartition_num):
    # Provision to override the columnnames and datatypes
    dbutils.fs.rm("dbfs:"+delta_dir, True)
    if format.lower() in ['orc','parquet']:
        l = ( len(partition_cols['normal']) + len(partition_cols['drop']) ) * ["*"]
        src_dir = os.path.join(src_dir,"merged",format.lower(),*l,"*","*","*")
        df = spark.read.format(format).load("dbfs:"+src_dir)
        columns_to_drop = ['ziw_row_id','ziw_seqval', 'ziw_scn', 'ziw_source_start_date', 'ziw_source_start_timestamp',
                           'ziw_target_start_date', 'ziw_source_end_date', 'ziw_source_end_timestamp',
                           'ziw_target_end_date', 'ziw_target_end_timestamp', 'ziw_active', 'ziw_status_flag', 'ziw_offset']
        df1 = df.drop(*columns_to_drop)
        df1 = df1.withColumnRenamed("ziw_target_start_timestamp", "ziw_target_timestamp")
        df1 = df1.withColumn("ziw_is_deleted", col("ziw_is_deleted").cast(BooleanType()))
        if src_type.lower() == "file":
            df1 = df1.withColumnRenamed("ziw_filename", "ziw_file_name")
            df1 = df1.withColumn("ziw_file_modified_timestamp", col("ziw_target_timestamp"))

        if len(override_columns) > 0:
            global override_dict
            override_dict = override_columns
            df1 = df1.transform(quinn.with_columns_renamed(rename_col))

        if len(partition_cols['normal']) > 0:
            if len(partition_cols['drop']) > 0:
                df1 = df1.drop(*partition_cols['drop'])
            #We do not support regex partitioning in DB. Hence dropping the columns which were present in data as part of regex partitioning in HDI
            if repartition_num !=-1:
                df1.repartition(repartition_num).write.partitionBy(*partition_cols['normal']).format("delta").mode(mode).save("dbfs:" + delta_dir)
            else:
                df1.write.partitionBy(*partition_cols['normal']).format("delta").mode(mode).save("dbfs:" + delta_dir)
        else:
            if repartition_num != -1:
                df1.repartition(repartition_num).write.format("delta").mode(mode).save("dbfs:" + delta_dir)
            else:
                df1.write.format("delta").mode(mode).save("dbfs:" + delta_dir)

        try:
            drop_table_cmd = 'drop table if exists {}.{}'.format(source_schema,table_name)
            spark.sql(drop_table_cmd)
        except Exception:
            print("Error while dropping table")

        try:
            cmd = 'create database {}'.format(source_schema)
            spark.sql(cmd)
        except Exception as e:
            print("Error while creating database ")

        try:
            cmd = 'create table {}.{} using delta location "{}"'.format(source_schema,table_name,"dbfs:"+delta_dir)
            spark.sql(cmd)
        except Exception:
            print("Error while creating table")
    else:
        #This part to handle CSV, XML, JSON format
        pass

def main():
    if len(sys.argv) == 11:
        src_dir=sys.argv[1]
        delta_dir=sys.argv[2]
        mode=sys.argv[3]
        source_schema = sys.argv[4]
        table_name = sys.argv[5]
        src_type = sys.argv[6]
        format = sys.argv[7]
        part_cols_string = sys.argv[8]
        partition_cols = json.loads(part_cols_string)
        override_columns = json.loads(sys.argv[9])
        repartition_num = int(sys.argv[10])
        print("The cmdline arguments are ")
        LOGGER.info("The cmdline arguments are ")
        print(sys.argv)
        LOGGER.info(sys.argv)
        print("Starting conversion of Files to Delta now")
        LOGGER.info("Starting conversion of Files to Delta now")
        set_spark_confs()
        convert_to_delta(src_dir,delta_dir,mode,source_schema,table_name,src_type,format,partition_cols,override_columns,repartition_num)
        print("Conversion of ORC/Parquet to Delta Done!!!")
        LOGGER.info("Conversion of ORC/Parquet to Delta Done!!!!!!")
    else:
        print("Insufficient or more arguments passed")
        sys.exit(1)

main()


