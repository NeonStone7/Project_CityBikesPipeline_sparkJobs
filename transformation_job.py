from src.commons import (
    save_to_iceberg,
    remote_spark_session,

)
import argparse
from src.definitions import definitions

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--table_name', type = str, default = '')
    parser.add_argument('--source_bucket', type = str, default = '')
    parser.add_argument('--date', type = str, default = '')
    parser.add_argument('--network_name', type = str, default = '')
    parser.add_argument('--destination_bucket', type = str, default = '')

    args = parser.parse_args()

    spark = remote_spark_session(args.table_name)

    source_bucket = args.source_bucket
    network_name = args.network_name
    date = args.date
    destination_bucket = args.destination_bucket
    table_name = args.table_name

    schema = definitions[args.table_name]['schema']
    fields = definitions[args.table_name]['fields']

    # set args
    source_path = f"s3://{source_bucket}/{network_name}/{date}/{network_name}.json"
    destination_path = f"s3://{destination_bucket}/{schema}/{table_name}/"

    # read from source
    df = spark.read.parquet(source_path)

    # transform data
    transformed_df = definitions[args.table_name]['transformer'](df)

    # save to iceberg
    save_to_iceberg(spark, transformed_df, table_name, schema, fields, destination_path)

