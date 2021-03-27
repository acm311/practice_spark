from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date

spark = SparkSession.\
    builder.\
    appName('fifa_spk').\
    getOrCreate()

df_players_21 = spark.read.csv('fifa_data/players_21.csv', sep=',', header=True)

#Display first 10 rows
df_players_21.show(10)

#Num of rows
print(df_players_21.count())

#select columns
df_players = df_players_21.select('short_name', 'dob', 'height_cm', 'weight_kg', 'nationality',
                                  'club_name', 'league_name', 'league_rank', 'overall', 'potential', 'value_eur', 'player_positions',
                                  'preferred_foot', 'player_tags', 'team_position', 'team_jersey_number',
                                  'joined')

df_players.show(10)

#Obtain age column based on dob
df_players.printSchema()
#df_players = df_players.withColumn('dob', col('dob').cast(DateType()))
df_players = df_players.withColumn('dob', to_date('dob', 'yyyy-MM-dd'))
df_players.printSchema()
today = date.today()
df_players = df_players.withColumn('today', lit(str(today)))
df_players.show(10)

df_players = df_players.withColumn('age', floor(datediff('today', 'dob')/365.25)).drop('today')
df_players.show(10)

#Best colombian players today
df_players.filter("nationality = 'Colombia'").sort(desc('overall')).show(20)

#Show league names
df_players.select('league_name').distinct().orderBy('league_name').show(10)

#testing filtering data
#df_players.where("club_name = 'Boca Juniors'").show(10)
df_players.selectExpr('short_name', "nationality == 'Germany' isGerman").show(10)
df_players.select('short_name', expr("club_name like '%Boca%' club_contains_boca_word")).show(10)

#Players on the colombian league
df_players.where(upper(col('league_name')).like('%COLOMBIA%')).show(10)

#French players with value_eur > 50.000.000
df_players = df_players.withColumn('value_eur', col('value_eur').cast(IntegerType()))
df_players.printSchema()

df_players.where("nationality = 'France' and value_eur > '50000000'").show(10)