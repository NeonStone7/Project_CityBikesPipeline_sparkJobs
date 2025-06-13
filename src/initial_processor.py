from pyspark.sql.functions import *
from commons import (
    local_spark_session
)

spark = local_spark_session('processor')

json_path = "resources/json_output_abu_dhabi_careem_bike.json"
df = spark.read.option("multiline", "true").json(json_path)

def initial_processing(df):
        
    df = (
        df.selectExpr('network.id as network_id', 'network.name as network_name', 
                'network.location.latitude as network_latitude',
                'network.location.longitude as network_longitude',
                'network.location.city as network_city',
                'network.location.country as network_country',
                'network.href as network_href', 
                'network.company as network_company',
                'network.gbfs_href as network_gbfs_href', 
                'network.stations as network_stations',)
        .withColumn('network_company',col('network_company').getItem(0))
        .withColumn('network_stations', explode(col('network_stations')))
        .selectExpr('*', 
                    'network_stations.id as station_id',
                    'network_stations.name as station_name',
                    'network_stations.latitude as station_latitude',
                    'network_stations.longitude as station_longitude',
                    'network_stations.timestamp as station_timestamp',
                    'network_stations.free_bikes as station_free_bikes',
                    'network_stations.empty_slots as station_empty_slots',
                    )
        .drop('network_stations')
        .withColumn('processing_timestamp', current_timestamp())
        .withColumn('year', year(col('processing_timestamp')))
        .withColumn('month', month(col('processing_timestamp')))
        .withColumn('day', day(col('processing_timestamp')))
    )

    return df

