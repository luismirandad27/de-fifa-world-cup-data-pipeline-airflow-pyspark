#!/usr/bin/env python
# coding: utf-8

# Import Libraries
from pyspark.sql.types import StructType, StructField, FloatType, BooleanType
from pyspark.sql.types import DoubleType, IntegerType, StringType
import pyspark
from pyspark.sql import SparkSession
import os

# Setup the Configuration (Adding jars for hadoop-aws and aws-java-sdk-bundle)
spark = SparkSession.builder\
            .appName("FifaWorldCup_Extraction")\
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
            .getOrCreate()
            

# Setting Aws configurations
spark.conf.set("fs.s3a.access.key", str(os.environ['AWS_ACCESS_KEY']))
spark.conf.set("fs.s3a.secret.key", str(os.environ['AWS_SECRET_ACCESS_KEY']))
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

print("**********************************")
print(str(os.environ['AWS_ACCESS_KEY']))
print(str(os.environ['AWS_SECRET_ACCESS_KEY']))
print("**********************************")

# Making a process to extract and store data into parquet files in /data/raw
def extract_datasets(lst_datasets):
    
    for filename in lst_datasets:
        
        df_data = spark\
                .read\
                .format("csv")\
                .option("inferSchema",True)\
                .option("header",True)\
                .load(f"s3a://fifa-world-cup-bucket/inputs/{filename}.csv")
        
        df_data\
             .write\
             .format("parquet")\
             .mode("overwrite")\
             .save(f"../../../app/data/raw/{filename}")


lst_datasets = ['WorldCupMatches','WorldCupPlayers','WorldCups']

extract_datasets(lst_datasets)



