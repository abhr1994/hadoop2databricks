import sys, os, json, re
from pyspark.sql.types import BooleanType, TimestampType
from pyspark.sql.functions import explode, col
from pyspark.sql.functions import lit
from datetime import datetime

import quinn

# Linking with Spark Log4j
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.setLevel(log4jLogger.Level.DEBUG)


def rename_col(s):
    if override_dict.get(s.lower()):
        return override_dict[s.lower()].lower()
    else:
        return s


def set_spark_confs():
    confs = open("/dbfs/FileStore/tables/confs/spark_defaults.conf").readlines()
    for conf in confs:
        key, value = conf.split()
        spark.conf.set(key.strip(), value.strip())


def convert_to_delta(ext_src_schema,ext_table_name,delta_dir, mode, source_schema, table_name, src_type, format, partition_cols,
                     override_columns, repartition_num,src_dir,is_iw_table):
    dbutils.fs.rm("dbfs:" + delta_dir, True)
    if format.lower() in ['orc', 'parquet']:
        if src_dir.lower() != "n/a":
            if is_iw_table.lower() == "true":
                l = len(partition_cols) * ["*"]
                src_dir = os.path.join(src_dir, "merged", format.lower(), *l, "*", "*", "*")
            df = spark.read.format(format).load("dbfs:"+src_dir)
        else:
            df = spark.sql("select * from {}.{}".format("pep_migration_" + ext_src_schema, ext_table_name))
        ziw_cols = [i for i in df.columns if i.startswith('ziw')]
        df1 = df.withColumn("ziw_target_timestamp", lit(str(datetime.now())).cast(TimestampType()))
        df1 = df1.withColumn("ziw_is_deleted", lit("false").cast(BooleanType()))
        if src_type.lower() == "file":
            if "ziw_filename" in ziw_cols:
                df1 = df1.withColumnRenamed("ziw_filename", "ziw_file_name")
            else:
                df1 = df1.withColumn("ziw_filename", lit("file_name_not_found"))
            df1 = df1.withColumn("ziw_file_modified_timestamp", col("ziw_target_timestamp"))
        if len(override_columns) > 0:
            global override_dict
            override_dict = dict((k.lower(), v) for k, v in override_columns.items())
            def change_col_name(s):
                return s in override_dict
            df1 = df1.transform(quinn.with_some_columns_renamed(rename_col, change_col_name))
        if len(partition_cols) == 0 or (len(partition_cols) == 1 and partition_cols[0].lower() == 'n/a'):
            if repartition_num != -1:
                df1.repartition(repartition_num).write.format("delta").mode(mode).save("dbfs:" + delta_dir)
            else:
                df1.write.format("delta").mode(mode).save("dbfs:" + delta_dir)
        else:
            if repartition_num != -1:
                df1.repartition(repartition_num).write.partitionBy(*partition_cols).format("delta").mode(mode).save("dbfs:" + delta_dir)
            else:
                df1.write.partitionBy(*partition_cols).format("delta").mode(mode).save("dbfs:" + delta_dir)
    else:
        print("Unsupported format {}".format(format))
        return None

    try:
        drop_table_cmd = 'drop table if exists {}.{}'.format(source_schema, table_name)
        spark.sql(drop_table_cmd)
    except Exception:
        print("Error while dropping table")

    try:
        cmd = 'create database {}'.format(source_schema)
        spark.sql(cmd)
    except Exception as e:
        print("Error while creating database ")

    try:
        cmd = 'create table {}.{} using delta location "{}"'.format(source_schema, table_name, "dbfs:" + delta_dir)
        spark.sql(cmd)
    except Exception:
        print("Error while creating table")

def main():
    if len(sys.argv) == 14:
        ext_src_schema = sys.argv[1]
        ext_table_name = sys.argv[2]
        delta_dir = sys.argv[3]
        mode = sys.argv[4]
        source_schema = sys.argv[5]
        table_name = sys.argv[6]
        src_type = sys.argv[7]
        format = sys.argv[8]
        part_cols_string = sys.argv[9]
        if part_cols_string == "":
            partition_cols = []
        else:
            partition_cols = part_cols_string.split(',')
        override_columns = json.loads(sys.argv[10])
        repartition_num = int(sys.argv[11])
        src_dir = sys.argv[12]
        is_iw_table = sys.argv[13]
        print("The cmdline arguments are ")
        LOGGER.info("The cmdline arguments are ")
        print(sys.argv)
        LOGGER.info(sys.argv)
        print("Starting conversion of Files to Delta now")
        LOGGER.info("Starting conversion of Files to Delta now")
        set_spark_confs()
        convert_to_delta(ext_src_schema,ext_table_name,delta_dir, mode, source_schema, table_name, src_type, format, partition_cols,
                         override_columns, repartition_num,src_dir,is_iw_table)
        print("Conversion of ORC/Parquet to Delta Done!!!")
        LOGGER.info("Conversion of ORC/Parquet to Delta Done!!!!!!")
    else:
        print("Insufficient or more arguments passed")
        sys.exit(1)

main()
