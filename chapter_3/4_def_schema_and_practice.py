from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, concat
from pyspark.sql import functions as F

#Define schema for our data using DDL (Dtaa Definition Language)
schema = "`ID` INT, `First` STRING, `Last` STRING, `Url` STRING," \
         "`Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"

#Create our static data
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
        [2, "Brooke", "Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
        [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web", "twitter", "FB", "LinkedIn"]],
        [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
        [5, "Matei", "Zaharia", "https://tinyurl.5", "5/4/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
        [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]]

# Main program
if __name__ == "__main__":
    #Create a SparkSession
    spark = (SparkSession
             .builder
             .appName("Example-3_6")
             .getOrCreate())

    #Create a DataFrame using the schema defined above
    blogs_df = spark.createDataFrame(data, schema)

    blogs_df.show()

    print(blogs_df.columns)

    #Access a particular column with col and it returns a Column Type
    print(blogs_df["ID"])
    print(blogs_df.select(F.col("ID")))


    #Use an expression to compute a value
    blogs_df.select(expr("Hits * 2")).show()
    blogs_df.select(expr("Hits * 2")).show(2)

    #or use col to cumpute value        --> another way
    blogs_df.select(F.col("Hits") * 2).show(2)

    blogs_df.select(blogs_df["Hits"] * 2).show(2)

    #Use an expression to compute big hitters for blogs
    #This adds a new column, Big Hitters, based on the conditional expression
    blogs_df.withColumn("BigHitters", expr("Hits > 10000")).show()

    # Concatenate three columns, create a new column, and show the newly created concatenated column
    blogs_df.withColumn("AuthorsId", concat(expr("First"), expr("Last"), expr("ID")))\
        .select("AuthorsId")\
        .show(4)

    # These statements return the same value, showing that
    # expr is the same as a col method call
    blogs_df.select(expr("Hits")).show(2)
    blogs_df.select(F.col("Hits")).show(2)
    blogs_df.select("Hits").show(2)

    # Sort by column "Id" in descending order
    blogs_df.sort(F.col("Id").desc()).show()
    blogs_df.sort(blogs_df["Id"].desc()).show()
