# In Python, define a schema
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# Programmatic way to define a schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                            StructField('UnitID', StringType(), True),
                            StructField('IncidentNumber', IntegerType(), True),
                            StructField('CallType', StringType(), True),
                            StructField('CallDate', StringType(), True),
                            StructField('WatchDate', StringType(), True),
                            StructField('CallFinalDisposition', StringType(), True),
                            StructField('AvailableDtTm', StringType(), True),
                            StructField('Address', StringType(), True),
                            StructField('City', StringType(), True),
                            StructField('Zipcode', IntegerType(), True),
                            StructField('Battalion', StringType(), True),
                            StructField('StationArea', StringType(), True),
                            StructField('Box', StringType(), True),
                            StructField('OriginalPriority', StringType(), True),
                            StructField('Priority', StringType(), True),
                            StructField('FinalPriority', IntegerType(), True),
                            StructField('ALSUnit', BooleanType(), True),
                            StructField('CallTypeGroup', StringType(), True),
                            StructField('NumAlarms', IntegerType(), True),
                            StructField('UnitType', StringType(), True),
                            StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                            StructField('FirePreventionDistrict', StringType(), True),
                            StructField('SupervisorDistrict', StringType(), True),
                            StructField('Neighborhood', StringType(), True),
                            StructField('Location', StringType(), True),
                            StructField('RowID', StringType(), True),
                            StructField('Delay', FloatType(), True)])

spark = (SparkSession
         .builder
         .appName("test_read")
         .getOrCreate())

# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

fire_df.show(n=10)

print(fire_df.schema)

#Save data as a parquet file
parquet_file = "result.parquet"
fire_df.write.format("parquet").mode("overwrite").save(parquet_file)

#Save data as a table
parquet_table = "parquet_table"
fire_df.write.format("parquet").saveAsTable(parquet_table)

