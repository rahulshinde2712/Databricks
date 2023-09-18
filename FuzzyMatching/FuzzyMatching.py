# Databricks notebook source
"""
Import required liabrary like : fuzzywuzzy
In ShotText file : Material, Material Type, Business Unit, Material Description as Short Text
"""


import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import col, length, levenshtein
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from pyspark.sql.types import *
spark = SparkSession.builder.appName('sttring_matching').getOrCreate() 

# COMMAND ----------


#Read the Required file
rawDF = spark.read.format('com.databricks.spark.csv').options(header = 'true', inferschema = 'true').load("/FileStore/tables/ShortText.csv",header= True)
df1 = rawDF.select(col("Short Text").alias("ShortText"))

df1 = df1.select("ShortText").distinct()
df2 = df1.withColumn("ShortText2", col("ShortText"))
df2 = df2.select("ShortText2")
#apply Crossjoin 
df3 = df1.crossJoin(df2)

# COMMAND ----------

def fuzzDef(i,j):
  return fuzz.token_sort_ratio(i,j)

#Define a percentage for matching
fuzzUdf = udf(fuzzDef, LongType())
df3 = df3.withColumn("Ratio", fuzzUdf(df3.ShortText,df3.ShortText2)).where(col("Ratio")>70)

# COMMAND ----------

#Write file in dbfs
df3.write.format("com.databricks.spark.csv").option("header","true").save("dbfs:/FileStore/FuzzyOutput/Result_File.csv" ,mode='overwrite')
