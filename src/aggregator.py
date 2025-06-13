from pyspark.sql.functions import *
from commons import (
    local_spark_session
)
from initial_processor import initial_processing

spark = local_spark_session('processor')

json_path = "resources/json_output_abu_dhabi_careem_bike.json"
df = spark.read.option("multiline", "true").json(json_path)

new_df = initial_processing(df)
print(new_df.show())

needed_cols = [column for column in new_df.columns if 'station' in column and ('free_bikes' not in column or 'empty_slots' not in column)]
print(new_df.select(*needed_cols).show())