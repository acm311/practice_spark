from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.\
        builder.\
        appName("test_1").\
        getOrCreate()

    file_name = "mnm_dataset.csv"

    mnm_df = spark.read.csv(file_name, header=True)
    #mnm_df.show(n=5)

    df_TX = mnm_df.filter("State = 'TX'")
    #df_TX.show()

    df_TX_Blue = mnm_df.filter("State = 'TX' and Color = 'Blue'")
    #print(df_TX_Blue.count())

    df_TX_Blue_columnCount = mnm_df.filter("State = 'TX' and Color = 'Blue'").select('Count')
    #df_TX_Blue_columnCount.show()

    df_TOTAL_TX_Blue = mnm_df.filter("State = 'TX' and Color = 'Blue'").agg(sum('Count'))
    df_TOTAL_TX_Blue.show()

    fff = mnm_df.select("State", "Color", "Count").groupBy('State', 'Color').agg(count("Count"))
    fff.show()

    ggg = mnm_df.select("State", "Color", "Count").groupBy('State', 'Color').agg(sum("Count")).orderBy('State', 'Color')
    ggg.show(n=1000)

    ooo = mnm_df.where("State = 'TX' and Color = 'Red'")
    ooo.show()

    jjj = mnm_df.where((mnm_df.State == 'TX') & (mnm_df.Color == 'Red'))
    jjj.show()

    rrr = mnm_df.filter("State = 'TX' and Color = 'Red'")
    rrr.show()
