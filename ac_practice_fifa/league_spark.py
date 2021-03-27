from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def getWinner(team1, team2, goals_team1, goals_team2):
    if (goals_team1 > goals_team2):
        return team1
    elif (goals_team2 > goals_team1):
        return team2
    return ''

spark = SparkSession.builder.appName('league_spk').getOrCreate()

df_leagues = spark.read.csv('leagues_data/BIG_FIVE_1995_2019.csv', header=True, sep=',')

df_leagues.show(10)

#Create a column winner
getWinner_udf = spark.udf.register("getWinner_udf", getWinner)

df_leagues = df_leagues.withColumn('winner', getWinner_udf('Team 1', 'Team 2', 'FT Team 1', 'FT Team 2'))
df_leagues.show(10)

#Teams with more victories...
df_leagues = df_leagues.groupBy('winner').agg(count('winner').alias('Num. victories')).sort(desc(count('winner')))
df_leagues.show(10)

