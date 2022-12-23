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
            .appName("FifaWorldCup_Loading_GoldLayer")\
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
            .getOrCreate()

df_wc_info = spark.read.format("parquet").load("../../../app/data/transformed/WorldCupMain")
df_scorers = spark.read.format("parquet").load("../../../app/data/transformed/WorldCupScorers")
df_teams = spark.read.format("parquet").load("../../../app/data/transformed/WorldCupTeams")

spark.conf.set("fs.s3a.access.key", str(os.environ['AWS_ACCESS_KEY']))
spark.conf.set("fs.s3a.secret.key", str(os.environ['AWS_SECRET_ACCESS_KEY']))
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

df_teams.printSchema()

#World Cups Thropies per country
df_wc_per_country = df_wc_info\
                    .groupBy('Winner')\
                    .agg(F.count('*').alias('total_world_cups'))

df_wc_per_country.show()

#World Cup'Top Scorers
partitionByColumn = Window.orderBy(F.col('total_goals').desc())


df_wc_top_scorers = df_scorers\
                    .groupBy('player_name','team_name')\
                    .agg(F.sum('total_goals').alias('total_goals'),F.sum('total_penalty_goals').alias('total_penalty_goals'))\
                    .select(
                        'player_name',
                        'team_name',
                        'total_goals',
                        'total_penalty_goals',
                        F.dense_rank().over(partitionByColumn).alias('ranking')
                    )

df_wc_top_scorers.show()

conversion_name = F.when(F.col('team_name').like('Germany%'),F.lit('Germany')).otherwise(F.col('team_name')).alias('team_name')

df_wc_id = df_wc_info.select(F.col('Year').alias('wc_year'),F.col('ranking_wc'))

df_teams_f1 = df_teams\
.withColumn('team_name',conversion_name)\
.join(df_wc_id,['wc_year'],'left_outer')\
.withColumn('next_wc_id',F.col('ranking_wc')+1)


df_teams_f2 = df_teams_f1\
.select(conversion_name,'WT','team_goals','wc_name','wc_year','final_stage','ranking_wc','next_wc_id').alias('a')\
.where(F.col('final_stage') == 'Champion')\
.join(df_teams_f1.select(
        F.col('final_stage').alias('final_stage_next_wc'),
        conversion_name.alias('team_name_next_wc'),
        F.col('wc_name').alias('wc_name_next_wc'),
        'ranking_wc'
      ).alias('b'),F.expr('a.next_wc_id == b.ranking_wc and a.team_name == b.team_name_next_wc'),how='left_outer')\
.withColumn('final_stage_next_wc',F.when(F.col('wc_name_next_wc').isNull(),F.when(F.col('next_wc_id')>20,F.lit('No WC Available'))\
                                                                  .otherwise(F.lit('Not Qualified')))\
                            .otherwise(F.col('final_stage_next_wc')))\
.drop('ranking_wc','next_wc_id','team_name_next_wc','wc_name_next_wc')

#Sending datasets to Amazon S3
df_teams_f2\
    .write\
    .format('parquet')\
    .mode('overwrite')\
    .save('s3a://fifa-world-cup-bucket/outputs/WorldCupTeams')

df_wc_top_scorers\
    .write\
    .format('parquet')\
    .mode('overwrite')\
    .save('s3a://fifa-world-cup-bucket/outputs/WorldCupScorers')

df_wc_info.write\
    .format('parquet')\
    .mode('overwrite')\
    .save('s3a://fifa-world-cup-bucket/outputs/WorldCupGeneral')



