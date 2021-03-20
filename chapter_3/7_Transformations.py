# In Python, define a schema
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *

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


# PROJECTIONS AND FILTERS
few_fire_df = fire_df\
                .select("IncidentNumber", "AvailableDtTm", "CallType")\
                .where(F.col("CallType") != "Medical Incident")
few_fire_df.show(5, truncate=False)

#another way        (USING FILTER)
few_fire_df = fire_df\
                .select("IncidentNumber", "AvailableDtTm", "CallType")\
                .filter("CallType != 'Medical Incident'")
few_fire_df.show(5, truncate=False)

#how many distinct CallTypes were recorded as the causes of fire calls?
fire_df.select("CallType")\
        .where(F.col("CallType").isNotNull())\
        .agg(countDistinct("CallType").alias("DistinctCallTypes"))\
        .show()

fire_df.select("CallType")\
        .filter("CallType IS NOT NULL")\
        .agg(countDistinct("CallType").alias("DistinctCallTypes"))\
        .show()

# Filter for only distinct non-null CallTypes from all the rows
fire_df.select("CallType")\
        .where(F.col("CallType").isNotNull())\
        .distinct()\
        .show(10, False)


# RENAMING, ADDING, AND DROPPING COLUMNS
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
new_fire_df.select("ResponseDelayedinMins")\
            .where(F.col("ResponseDelayedinMins") > 5)\
            .show(5, False)

#Date and time columns...
fire_ts_df = new_fire_df\
                .withColumn("IncidentDate", to_timestamp(F.col("CallDate"), "MM/dd/yyyy"))\
                .drop("CallDate")\
                .withColumn("OnWatchDate", to_timestamp(F.col("WatchDate"), "MM/dd/yyyy"))\
                .drop("WatchDate") \
                .withColumn("AvailableDtTm", to_timestamp(F.col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")) \
                .drop("WatchDate")

fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTm")\
            .show(5, False)

fire_ts_df.select(year("IncidentDate"))\
        .distinct()\
        .orderBy(year("IncidentDate"))\
        .show()

#AGGREGATIONS
fire_ts_df.select("CallType")\
        .where(F.col("CallType").isNotNull())\
        .groupBy("CallType")\
        .count()\
        .orderBy("count", ascending=False)\
        .show(n=10, truncate=False)

fire_ts_df.select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"), F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))\
            .show()