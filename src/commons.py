from pyspark.sql import SparkSession

def local_spark_session(name):
    return (
        SparkSession.builder
        .config('hive.exec.dynamic.partition.mode', 'nonstrict')
        .config('spark.sql.source.partitionOverwriteMode', 'dynamic')
        .config('spark.master', 'local[*]')
        .appName(name)
        .getOrCreate()
    )

def remote_spark_session(name):
    return (
        SparkSession.builder
        .config('hive.exec.dynamic.partition.mode', 'nonstrict')
        .config('spark.sql.source.partitionOverwriteMode', 'dynamic')
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')
        .config('spark.sql.catalog.spark_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
        .config('spark.sql.catalog.spark_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .config('spark.sql.legacy.timeParserPolicy', 'LEGACY')
        .config('spark.sql.sources.partitionColumnTypeInference.enabled', 'false')
        .config('spark.sql.catalog.spark_catalog.http-client.apache.max-connections', '3000')
        .appName(name)
        .getOrCreate()
    )

def save_to_iceberg(spark_session, df, table_name, schema, fields, location, partition = 'year,month,day', catalog='spark_catalog'):

    # create schema
    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # createtable
    
    new_fields = ""
    for field, dtype in fields.items():
        new_fields = new_fields + f"{field} {dtype}, "
    cmd = (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {catalog}.{schema}.{table_name}"
        f" ({new_fields[:-2]}) " + 
        f" USING ICEBERG PARTITIONED BY ({partition}) LOCATION '{location}'"
    )
    spark_session.sql(cmd)

    # write into table
    df.write.insertInto(f'{catalog}.{schema}.{table_name}')
