from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *

#Create a Spark session
spark = SparkSession.builder.appName('SparkSQLExampleApp').getOrCreate()

#Path to data set
csv_file = 'chapter_4/departuredelays.csv'

#Read and create a temporary view
#Infer schema (note that for larger files you may want to specify the schema)
df = spark.read.format('csv').option('inferSchema', 'true').option('header', 'true').load(csv_file)
df.createOrReplaceTempView('us_delay_flights_tbl')

#SAPRK SQL
spark.sql("""SELECT distance, origin, destination \
            FROM us_delay_flights_tbl \
            WHERE distance > 1000 ORDER BY distance DESC""").show(10)

#DATAFRAME API equivalent
print('DATAFRAME API equivalent...')
df.select('distance', 'origin', 'destination').where(col('distance') > 1000).orderBy(desc('distance')).show(10)


spark.sql("""SELECT date, delay, origin, destination \
            FROM us_delay_flights_tbl
            WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
            ORDER by delay DESC""").show(10)

df.printSchema()

#tmp_df = df.withColumn("flight_date", to_timestamp(col("date").cast(StringType()), "MM/dd/yyyy")).drop("date")
#tmp_df.show(30)

#Define a UDF to convert the date format into a legible format
def to_date_format(d_str):
    l = [char for char in d_str]
    return "".join(l[0:2]) + "/" + "".join(l[2:4]) + " " + " " + "".join(l[4:6]) + ":" + "".join(l[6:])

print(to_date_format("02190925"))

#change Datatype of one column (int -> string)
df = df.withColumn("date_string", col("date").cast(StringType())).drop("date")

#Register the UDF
spark.udf.register("to_date_format_udf", to_date_format, StringType())

# Test our UDF
df.selectExpr("to_date_format_udf(date_string) as data_format").show(10, truncate=False)

spark.sql("""SELECT delay, origin, destination, \
                CASE \
                    WHEN delay > 360 THEN 'Very Long Delays' \
                    WHEN delay > 120 AND delay < 360 THEN 'Long Delays' \
                    WHEN delay > 60 AND delay < 120 THEN 'Short Delays' \
                    WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delays' \
                    WHEN delay = 0 THEN 'No Delays' \
                    ELSE 'Early' \
                END AS Fligh_Delays \
                FROM us_delay_flights_tbl \
                ORDER BY origin, delay DESC """).show(10)

