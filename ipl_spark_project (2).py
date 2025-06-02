# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

spark=SparkSession.builder.appName('IPL_analysis').getOrCreate()

# COMMAND ----------

ball_by_ball_Scehma=StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True),
])
ball_by_ball_df=spark.read.schema(ball_by_ball_Scehma).format('csv').option('header',True).load("s3://ipl-data-analysis-project/Ball_By_Ball.csv")

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True),
])
match_df = spark.read.schema(match_schema) \
    .format('csv') \
    .option('header', True) \
    .load("s3://ipl-data-analysis-project/Match.csv")


# COMMAND ----------

Player_scehma= StructType([
    StructField("player_sk",IntegerType(),True),
    StructField("player_id",IntegerType(),True),
    StructField("player_name",StringType(),True),
    StructField("dob", StringType() ,True),
    StructField("batting_hand",StringType(),True),
    StructField("bowling_skill",StringType(),True),
    StructField("country_name",StringType(),True)
])
Player_df=spark.read.schema(Player_scehma).format("csv").option("header",True).load("s3://ipl-data-analysis-project/Player.csv")

# COMMAND ----------

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(20, 0), True),  # Adjust precision/scale if needed
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob",StringType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),  # 'year' stored as IntegerType
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True),
])
player_match_df = spark.read \
    .schema(player_match_schema) \
    .format('csv') \
    .option('header', True) \
    .load("s3://ipl-data-analysis-project/Player_match.csv")

# COMMAND ----------

team_scehma=StructType([
    StructField('team_sk',IntegerType(),True),
    StructField('team_id',IntegerType(),True),
    StructField('team_name',StringType(),True)

])
team_df=spark.read.schema(team_scehma).format('csv').option('header',True).load("s3://ipl-data-analysis-project/Team.csv")

# COMMAND ----------

team_df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###filtering to include valid delivers(Excluding the wides,legbyes,noballs,byes)

# COMMAND ----------

ball_by_ball_df = ball_by_ball_df.filter(
    (col('wides') == 0) & 
    (col('legbyes') == 0) & 
    (col('noballs') == 0) & 
    (col('byes') == 0)
)


# COMMAND ----------

ball_by_ball_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###total and average runs in each match and ining

# COMMAND ----------

avf_sum_total=ball_by_ball_df.groupBy('match_id','innings_no').agg(sum('runs_scored').alias('Total_scored'),avg('runs_scored').alias('Avergae_scored'))

# COMMAND ----------

avf_sum_total.display()

# COMMAND ----------

df_filled=ball_by_ball_df.fillna({'extra_runs': 0, 'wides': 0, 'legbyes': 0})

# COMMAND ----------

df_filled=ball_by_ball_df.fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ###flag  for high impact balls(either a wicket or more than 6 runs including extras)

# COMMAND ----------

ball_by_ball_df=ball_by_ball_df.withColumn('high_impact',when((col('runs_scored') + col('extra_runs') > 6) | (col('bowler_wicket')==True),True).otherwise(False) )

# COMMAND ----------

ball_by_ball_df=ball_by_ball_df.withColumn("is_wicket",when(col('out_type') != 'Not Applicable',1).otherwise(0))

# COMMAND ----------

match_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###convert the date into ISO format (yyyy-MM-dd)

# COMMAND ----------

match_df = match_df.withColumn("match_date", to_date(col("match_date"), "M/d/yyyy"))

# COMMAND ----------

match_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###extracting year,month,date for detailed time based analysis

# COMMAND ----------

match_df=match_df.withColumn('year',year('match_date'))
match_df=match_df.withColumn('year',month('match_date'))
match_df=match_df.withColumn('year',dayofmonth('match_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ###category of win margin

# COMMAND ----------

match_df=match_df.withColumn("win_margin",when(col('win_margin')>=31,"high").when((col('win_margin') >= 11) & (col('win_margin') <= 30), "medium").otherwise("low"))

# COMMAND ----------

match_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###impact of toss : analyze the toss and match winner

# COMMAND ----------

match_df=match_df.withColumn('Won_the_game',when(col('toss_winner') == col('match_winner'),'Yes').otherwise('No'))

# COMMAND ----------

Player_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###handling the missing value in batting hand and bowling skills

# COMMAND ----------

Player_df=Player_df.withColumn(
    'bowling_skill',
    when(
    col('bowling_skill').isNull() | (col('bowling_skill') == 'N/A') ,'Unknown'
).otherwise(col('bowling_skill'))
    )
Player_df=Player_df.withColumn('batting_hand',
                               when(
                                   col('batting_hand').isNull() | 
                                   (col('batting_hand') == 'N/A'),'Unknown'
                               ).otherwise(col('batting_hand')))

# COMMAND ----------

Player_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calculating the year since debuted

# COMMAND ----------

player_match_df=player_match_df.withColumn('Debutin_year',year(current_date())- col('season_year'))

# COMMAND ----------

player_match_df.display()

# COMMAND ----------

ball_by_ball_df.createOrReplaceTempView('ball_by_ball')
match_df.createOrReplaceTempView('match')
player_match_df.createOrReplaceTempView('player_match')
team_df.createOrReplaceTempView('team')
Player_df.createOrReplaceTempView('Player')

# COMMAND ----------

# MAGIC %md
# MAGIC Calculating Top scoring batsmen form each season

# COMMAND ----------

top_scoring_batsmen_per_season=spark.sql("""
SELECT pm.player_name,
m.season_year,
sum(b.runs_scored) AS total_runs

FROM ball_by_ball b 
JOIN match m on b.match_id=m.match_id
JOIN player_match pm on m.match_id=pm.match_id and b.striker = pm.player_id
GROUP BY p.player_name,m.season_year
ORDER BY m.season_year,total_runs DESC
""")

# COMMAND ----------

top_scoring_batsmen_per_season.show(50)

# COMMAND ----------

