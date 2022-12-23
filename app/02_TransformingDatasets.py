#!/usr/bin/env python
# coding: utf-8

# Import Libraries
from pyspark.sql.types import StructType, StructField, FloatType, BooleanType
from pyspark.sql.types import DoubleType, IntegerType, StringType
import pyspark.sql.functions as F
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import os


# Setup the Configuration (Adding jars for hadoop-aws and aws-java-sdk-bundle)
spark = SparkSession.builder\
            .appName("FifaWorldCup_Transforming")\
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
            .getOrCreate()

df_wc_info = spark.read.format("parquet").load("../../../app/data/raw/WorldCups/*")
df_players = spark.read.format("parquet").load("../../../app/data/raw/WorldCupPlayers/*").where(F.col('MatchId').isNotNull())
df_matches = spark.read.format("parquet").load("../../../app/data/raw/WorldCupMatches/*").distinct().where(F.col('MatchId').isNotNull())

df_wc_info.printSchema()
df_players.printSchema()
df_matches.printSchema()

window_partition = Window.orderBy(F.col('Year').asc())

df_wc_info = df_wc_info.withColumn('ranking_wc',F.row_number().over(window_partition))\
                       .withColumn('wc_name',F.concat(F.col('Country'),F.lit(' '),F.col('Year')))


df_matches_f = df_matches.select(
    'Year','Stage',
    F.col('Home Team Name').alias('Home'),
    F.col('Home Team Goals').alias('Home G'),
    F.col('Home Team Initials').alias('Home I'),
    F.col('Away Team Name').alias('Away'),
    F.col('Away Team Goals').alias('Away G'),
    F.col('Away Team Initials').alias('Away I'),
    'RoundID',
    'MatchID',
    'Stage',
    F.when(F.col('Stage').like('Preliminary%'),F.lit(1))\
     .when(F.col('Stage').like('Group%'),F.lit(2))\
     .when(F.col('Stage')=='First round',F.lit(3))\
     .when(F.col('Stage')=='Round of 16',F.lit(4))\
     .when(F.col('Stage')=='Quarter-finals',F.lit(5))\
     .when(F.col('Stage')=='Semi-finals',F.lit(6))\
     .when(F.col('Stage').like('Match for third%'),F.lit(7))\
     .when(F.col('Stage').like('Play-off for thir%'),F.lit(7))\
     .when(F.col('Stage')=='Third place',F.lit(7))\
     .when(F.col('Stage')=='Final',F.lit(8)).alias('ranking_match'),
    F.when(F.col('Win conditions').isNotNull(),F.split('Win conditions',' ')[0])\
    .otherwise(None).alias('tiebreak_winner')
                 )


df_teams_home = df_matches_f\
                .select(
                    F.col('MatchId').alias('wc_match_id'),
                    F.col('Year').alias('wc_year'),
                    F.col('Stage').alias('wc_stage'),
                    F.col('ranking_match'),
                    F.col('Home').alias('team_name'),
                    F.col('Home I').alias('team_initials'),
                    F.col('Home G').alias('team_goals'),
                    F.when(F.col('ranking_match').isin(2,3), F.when(F.col('Home G') > F.col('Away G'),F.lit('W'))\
                                                              .when(F.col('Home G') == F.col('Away G'),F.lit('D'))\
                                                              .otherwise(F.lit('L')))\
                     .otherwise(F.when(F.length(F.col('tiebreak_winner'))==0 ,F.when(F.col('Home G') > F.col('Away G'),F.lit('W'))\
                                                                      .when(F.col('Home G') == F.col('Away G'),F.lit('D'))\
                                                                      .otherwise(F.lit('L')))\
                                  .otherwise(F.when(F.col('Home') == F.col('tiebreak_winner'),F.lit('W'))\
                                              .otherwise(F.lit('L')))
                    ).alias('result'),
                    F.when(F.length(F.col('tiebreak_winner'))>0,F.when(F.col('Home') == F.col('tiebreak_winner'),F.lit('W'))\
                                                                  .otherwise(F.lit('L')))\
                     .otherwise(F.lit(None)).alias('result_tiebreak')
                )

df_teams_away = df_matches_f\
                .select(
                    F.col('MatchId').alias('wc_match_id'),
                    F.col('Year').alias('wc_year'),
                    F.col('Stage').alias('wc_stage'),
                    F.col('ranking_match'),
                    F.col('Away').alias('team_name'),
                    F.col('Away I').alias('team_initials'),
                    F.col('Away G').alias('team_goals'),
                    F.when(F.col('ranking_match').isin(2,3), F.when(F.col('Away G') > F.col('Home G'),F.lit('W'))\
                                                              .when(F.col('Away G') == F.col('Home G'),F.lit('D'))\
                                                              .otherwise(F.lit('L')))\
                     .otherwise(F.when(F.length(F.col('tiebreak_winner'))==0 ,F.when(F.col('Away G') > F.col('Home G'),F.lit('W'))\
                                                                      .when(F.col('Away G') == F.col('Home G'),F.lit('D'))\
                                                                      .otherwise(F.lit('L')))\
                                  .otherwise(F.when(F.col('Away') == F.col('tiebreak_winner'),F.lit('W'))\
                                              .otherwise(F.lit('L')))
                    ).alias('result'),
                    F.when(F.length(F.col('tiebreak_winner'))>0,F.when(F.col('Away') == F.col('tiebreak_winner'),F.lit('W'))\
                                                                  .otherwise(F.lit('L')))\
                     .otherwise(F.lit(None)).alias('result_tiebreak')
                )

df_teams = df_teams_home.unionAll(df_teams_away)

df_teams_f = df_teams\
            .join(
                df_wc_info.select(
                    F.col('Year').alias('wc_year'),
                    F.col('Country').alias('wc_country'),
                    F.col('Winner').alias('wc_1st'),
                    F.col('Runners-Up').alias('wc_2nd'),
                    F.col('Third').alias('wc_3rd'),
                    F.col('Fourth').alias('wc_4th'),
                    F.col('ranking_wc').alias('wc_id')
                ),
                ['wc_year'],
                how='left_outer'
            )

cond_winner = F.when(F.col('wc_1st') == F.col('team_name'),F.lit(1)).otherwise(F.lit(0)).alias('flg_champion')
wc_name = F.concat(F.col('wc_country'),F.lit(' '),F.col('wc_year')).alias('wc_name')

calc_mw = F.when(F.col('result') == 'W', F.lit(1)).otherwise(F.lit(0))
calc_md = F.when(F.col('result') == 'D', F.lit(1)).otherwise(F.lit(0))
calc_ml = F.when(F.col('result') == 'L', F.lit(1)).otherwise(F.lit(0))
calc_mw_tie = F.when(F.col('result_tiebreak') == 'W', F.lit(1)).otherwise(F.lit(0))
calc_ml_tie = F.when(F.col('result_tiebreak') == 'L', F.lit(1)).otherwise(F.lit(0))


df_teams_f1 = df_teams_f\
    .select(
        'wc_match_id',
        wc_name,
        'wc_year',
        'wc_country',
        'team_name',
        'team_goals',
        'team_initials',
        'result',
        'ranking_match',
        'wc_1st',
        'wc_2nd',
        'wc_3rd',
        'wc_4th',
        cond_winner,
        'result_tiebreak'
    )\
    .groupBy(wc_name,'wc_year','wc_country','team_name','team_initials',cond_winner,'wc_1st','wc_2nd','wc_3rd','wc_4th')\
    .agg(
            F.sum('team_goals').alias('team_goals'),
            F.sum(calc_mw).alias('W'),
            F.sum(calc_md).alias('D'),
            F.sum(calc_ml).alias('L'),
            F.sum(calc_mw_tie).alias('WT'),
            F.sum(calc_ml_tie).alias('LT'),
            F.max('ranking_match').alias('last_stage')
        )

replace_country_names = F.when(F.col('team_name') == 'C�te d\'Ivoire', F.lit('Cote d\'Ivoire'))\
                         .when(F.col('team_name').like('%Bosnia%'), F.lit('Bosnia and Herzegovina'))\
                         .otherwise(F.col('team_name'))


final_stage = F.when(F.col('last_stage').isin(1,2,3), F.lit('Group Stage'))\
              .when(F.col('last_stage') == 4, F.lit('Round of 16'))\
              .when(F.col('last_stage') == 5, F.lit('Quarter Finals'))\
              .when(F.col('last_stage').isin(6,7,8), F.when(F.col('team_name') == F.col('wc_1st'), F.lit('Champion'))\
                                                         .when(F.col('team_name') == F.col('wc_2nd'), F.lit('Runner Up'))\
                                                         .when(F.col('team_name') == F.col('wc_3rd'), F.lit('Third Place'))\
                                                         .when(F.col('team_name') == F.col('wc_4th'), F.lit('Fourth Place'))).alias('final_stage')

final_columns = ['wc_name','wc_year','wc_country','team_name','team_initials','W','D','L','WT','LT','team_goals',
                 'last_stage',final_stage]

df_teams_f2 = df_teams_f1\
        .select(final_columns)\
        .drop('last_stage')\
        .withColumn('team_name',replace_country_names)

wc_name = F.concat(F.col('Country'),F.lit(' '),F.col('Year')).alias('wc_name')

df_players_f =df_players.select(F.col('MatchId').alias('wc_match_id'),
                  F.col('Team Initials').alias('team_initials'),
                  F.col('Player Name').alias('player_name'),
                  'Event',
                  F.explode(F.split('Event',' ')).alias('Event_e'))\
.select('*',
        F.when(F.substring('Event_e',1,1) == 'G',F.lit(1))\
         .when(F.substring('Event_e',1,1) == 'P',F.lit(1))\
         .otherwise(F.lit(0)).alias('goal'),
        F.when(F.substring('Event_e',1,1) == 'P',F.lit(1))\
         .otherwise(F.lit(0)).alias('goal_penalty')
       )\
.join(df_teams_f['wc_year','team_name','team_initials','wc_match_id'],
      ['wc_match_id','team_initials'],
      how='left_outer').distinct()

wc_name = F.concat(F.col('country_name'),F.lit(' '),F.col('wc_year')).alias('wc_name')
replace_names = F.when(F.col('player_name') == 'PERI�I?', F.lit('PERISIC'))\
                 .when(F.col('player_name').like('PEL� %'), F.lit('PELÉ'))\
                 .when(F.col('player_name').like('ROM�RIO%'), F.lit('ROMARIO'))\
                 .when(F.col('player_name') == 'M�LLER', F.lit('MULLER'))\
                 .when(F.col('player_name') == 'D�EKO', F.lit('DZEKO'))\
                 .when(F.col('player_name') == 'Z� ROBERTO', F.lit('ZÉ ROBERTO'))\
                 .when(F.col('player_name') == 'CH. AR�NGUIZ', F.lit('CH. ARANGUIZ'))\
                 .when(F.col('player_name') == 'Hugo S�NCHEZ', F.lit('Hugo SÁNCHEZ'))\
                 .when(F.col('player_name') == 'URE�A M.', F.lit('URENA M.'))\
                 .when(F.col('player_name') == 'MATTH�US', F.lit('MATTHAUS'))\
                 .when(F.col('player_name') == 'SIM�O', F.lit('SIMAO'))\
                 .when(F.col('player_name') == 'SCH�RRLE', F.lit('SCHURRLE'))\
                 .when(F.col('player_name') == 'G�TZE', F.lit('GOTZE'))\
                 .when(F.col('player_name') == 'KAK�', F.lit('KAKÁ'))\
                 .when(F.col('player_name') == '�ZIL', F.lit('OZIL'))\
                 .when(F.col('player_name') == 'VR�AJEVI?', F.lit('VRSAJEVIC'))\
                 .when(F.col('player_name') == 'IBI�EVI?', F.lit('IBISEVIC'))\
                 .when(F.col('player_name') == 'ALLB�CK', F.lit('ALLBACK'))\
                 .when(F.col('player_name') == 'MAND�UKI?', F.lit('MANDZUKIC'))\
                 .when(F.col('player_name') == 'ALLB�CK', F.lit('ALLBACK'))\
                 .when(F.col('player_name') == 'ADEMIR DA GUIA', F.lit('ADEMIR'))\
                 .when(F.col('player_name').isin('D. FORLAN','D.FORLAN'), F.lit('FORLAN'))\
                 .when(F.col('player_name').like('%BATISTUTA%'), F.lit('Gabriel BATISTUTA'))\
                 .when(F.col('player_name').like('%KLINSMANN%'), F.lit('Juergen KLINSMANN'))\
                 .when(F.col('player_name').like('%VIERI%'), F.lit('Christian VIERI'))\
                 .when(F.col('player_name').like('%HENRY%'), F.lit('Thierry HENRY'))\
                 .when((F.col('team_initials') == 'POR') & (F.col('player_name').like('%RONALDO%')),F.lit('Cristiano RONALDO'))\
                 .otherwise(F.regexp_replace(F.col('player_name'),'\\?','C'))

df_players_f = df_players_f\
                .withColumn('team_name',replace_country_names)\
                .withColumn('player_name',replace_names)

df_players_f1 = df_players_f\
.join(df_wc_info[F.col('Year').alias('wc_year'),F.col('Country').alias('country_name')],['wc_year'],how='left_outer')\
.select(wc_name,'team_name','team_initials','player_name','goal','goal_penalty')\
.groupBy('wc_name','team_name','team_initials','player_name')\
.agg(F.sum('goal').alias('total_goals'),F.sum('goal_penalty').alias('total_penalty_goals'))\
.where(F.col('total_goals')>0)

df_players_f2 = df_players_f1\
.select('*',F.dense_rank().over(Window.partitionBy(['wc_name']).orderBy(F.col('total_goals').desc(),F.col('team_initials').asc())).alias('ranking'))

df_wc_info = df_wc_info.withColumn('Winner',F.when(F.col('Winner').like('%Germany%'), F.lit('Germany')).otherwise(F.col('Winner')))\
                       .withColumn('Runners-Up',F.when(F.col('Runners-Up').like('%Germany%'), F.lit('Germany')).otherwise(F.col('Runners-Up')))\
                       .withColumn('Third',F.when(F.col('Third').like('%Germany%'), F.lit('Germany')).otherwise(F.col('Third')))\
                       .withColumn('Fourth',F.when(F.col('Fourth').like('%Germany%'), F.lit('Germany')).otherwise(F.col('Fourth')))\
                       .withColumn('Champion_Same_Country',F.when(F.col('Winner') == F.col('Country'),F.lit(1)).otherwise(F.lit(0)))

#Storing Data into parquet files into /data/transformed folder
df_teams_f2\
    .write\
    .format('parquet')\
    .mode('overwrite')\
    .save("../../../app/data/transformed/WorldCupTeams")

df_players_f2\
    .write\
    .format('parquet')\
    .mode('overwrite')\
    .save("../../../app/data/transformed/WorldCupScorers")

df_wc_info\
    .write\
    .format('parquet')\
    .mode('overwrite')\
    .save("../../../app/data/transformed/WorldCupMain")



