# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import col, length, levenshtein
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from pyspark.sql.types import *
spark = SparkSession.builder.appName('sttring_matching').getOrCreate() 

# COMMAND ----------

rawDF = spark.read.format('com.databricks.spark.csv').options(header = 'true', inferschema = 'true').load("/FileStore/tables/ShortText.csv",header= True)
df1 = rawDF.select(col("Short Text").alias("ShortText"))

df1 = df1.select("ShortText").distinct()
df2 = df1.withColumn("ShortText2", col("ShortText"))
df2 = df2.select("ShortText2")
df3 = df1.crossJoin(df2)

# COMMAND ----------

def fuzzDef(i,j):
  return fuzz.token_sort_ratio(i,j)


fuzzUdf = udf(fuzzDef, LongType())
df3 = df3.withColumn("Ratio", fuzzUdf(df3.ShortText,df3.ShortText2)).where(col("Ratio")>70)

# COMMAND ----------

df3.write.format("com.databricks.spark.csv").option("header","true").save("dbfs:/FileStore/FuzzyOutput/Result_20200701.csv" ,mode='overwrite')
