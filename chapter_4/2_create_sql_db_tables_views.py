from pyspark.sql import SparkSession

spark = SparkSession.Builder.appName('createDB_Tables').getOrCreate()

#Path to data set
csv_file = 'chapter_4/departuredelays.csv'

spark.sql("CREATE DATABASE learn_spark_db")
SPARK.SQL("USE learn_spark_db")

#Creating a managed table
#spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, \
#            distance INT, origin STRING, destination STRING)")

#Creating a managed table using DataFrame API
schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema=schema)

flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

#Creating an unmanaged table
# spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, 
#             distance INT, origin STRING, destination STRING
#             USING csv OPTIONS (PATH 'departuredelays.csv') """)

#Creating an unmanaged table using DataFrame API
flights_df.write.option("path", "/tmp/data/us_flights_delay").saveAsTable("managed_us_delay_flights_tbl")


######
#Creating views (GLOBAL -visible across all SaprkSessions on a given cluster-)
spark.sql("""CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
                SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE
                origin = 'SFO' """)

#Creating views (SESSION-SCOPED -visible only to a single SparkSession-)
spark.sql("""CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
                SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE
                origin = 'JFK' """)

#same thing using Dataframe API
df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")

df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

#to access a global temporary view use the prefix global_temp.<view_name>
# SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view

spark.read.table("us_origin_airport_JFK_tmp_view")
spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view")

#drop view
# DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view;
# DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view;

spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

#Viewing the Metadata
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("us_delay_flights_tbl")

#Reading Tables into DataFrames
us_flights_df = spark.sql("SELECT * FROM us_delays_flights_tbl")
us_flights_df2 = spark.table("us_delays_flights_tbl")


#reading parquet fils into a spark SQL table
# -- In SQL
# CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# USING parquet
# OPTIONS (
# path "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/" )