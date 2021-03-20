from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName('test_2').getOrCreate()

    # create Dataframe
    df = spark.createDataFrame([('Alvaro', 40), ('Yuly', 37), ('Chelin', 33), ('Nanin', 38)], ['name', 'age'])

    df.show()

    df_avg = df.agg(avg('age').alias('AVG'))
    df_avg.show()

    df.write.format('parquet').mode('overwrite').saveAsTable('df_TABLE')
    test_sql = spark.sql('select * from df_TABLE order by age')
    test_sql.show()