from pyspark.sql.functions import *
# from commons import (
#     local_spark_session
# )
# from initial_processor import initial_processing

# spark = local_spark_session('processor')

# json_path = "resources/json_output_abu_dhabi_careem_bike.json"
# df = spark.read.option("multiline", "true").json(json_path)

# new_df = initial_processing(df)
# print(new_df.show())

def create_agg_stations(spark, catalog, schema, table_name, year, month, day):
    df = spark.sql(f"select * from {catalog}.{schema}.{table_name} where year={year} and month={month} and day={day}")
    needed_cols = [column for column in df.columns if 'station' in column and 'free_bikes' not in column and 'empty_slots' not in column]
    stations_df = (df.select(*needed_cols)
                    .withColumn('processing_timestamp', current_timestamp())
                    .withColumn('year', year(col('processing_timestamp')))
                    .withColumn('month', month(col('processing_timestamp')))
                    .withColumn('day', day(col('processing_timestamp')))
                )
    
    return stations_df
    

def create_agg_networks(spark, catalog, schema, table_name, year, month, day):
    df = spark.sql(f"select * from {catalog}.{schema}.{table_name} where year={year} and month={month} and day={day}")
    needed_cols = [column for column in df.columns if 'network' in column]
    networks_df = (df.select(*needed_cols)
                    .withColumn('processing_timestamp', current_timestamp())
                    .withColumn('year', year(col('processing_timestamp')))
                    .withColumn('month', month(col('processing_timestamp')))
                    .withColumn('day', day(col('processing_timestamp')))
                )
    
    return networks_df

def create_bike_activity(spark, catalog, schema, table_name, year, month, day):
    df = spark.sql(f"select * from {catalog}.{schema}.{table_name} where year={year} and month={month} and day={day}")

    networks_df = (
        df
        .withColumn('station_timestamp', to_timestamp('station_timestamp', "yyyy-MM-dd'T'HH:mm:ss"))
        .selectExpr('network_id', 'station_id', 
                'extract(day from station_timestamp) as event_day',
                'station_timestamp', 'station_free_bikes', 'station_empty_slots'
                )
        .withColumn('hour', split(split(col('station_timestamp').cast('string'), 'T').getItem(1), ':').getItem(0) )
        .drop('station_timestamp')
        .distinct()
        .groupby('network_id', 'station_id','event_day', 'hour').agg(sum('station_free_bikes').alias('num_free_bikes'),
                                                               sum('station_empty_slots').alias('num_empty_slots'))
        .withColumn('processing_timestamp', current_timestamp())
        .withColumn('year', year(col('processing_timestamp')))
        .withColumn('month', month(col('processing_timestamp')))
        .withColumn('day', day(col('processing_timestamp')))
        )
    
    return networks_df
