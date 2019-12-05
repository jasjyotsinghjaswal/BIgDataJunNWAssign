# Import statements
from pyspark.sql import SparkSession, HiveContext
from assignUtilities import *
import logging
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pandas as pd
from tabulate import tabulate
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window
import json
import sqlite3
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
from datetime import datetime

# Create Spark handler
spark = SparkSession.builder.appName('JNAssignt').enableHiveSupport().getOrCreate()

# (1)-Start  Writiing Logs to HDFS Using custom function logToHDFS
logToHDFS("Starting Spark Job!!!!!!", "/user/logger/python/logs")

# (2)-Set Hive Properties
logToHDFS("Setting Hive Properties!!!!!!", "/user/logger/python/logs")
spark.sql("SET spark.hadoop.hive.merge.mapredfiles = false")
spark.sql("SET spark.hadoop.hive.merge.smallfiles.avgsize = 16000000")
spark.sql("SET spark.hadoop.hive.execution.engine = mr")

# (3)-Read Email Dataset as Spark Dataframe and Pandas Dataframe and Validate Emails
logToHDFS("Transformations to initiate Email Validation using Spark and Pandas", "/user/logger/python/logs")
email_spark_dataset = spark.read.option("header", "true").csv("dataset\\EmailData.csv")
email_spark_dataset.show(truncate=False)
mail_validator_udf = udf(mail_validator, StringType())
email_spark_dataset_validate = email_spark_dataset.withColumn("EmailFlag", mail_validator_udf("mail_id"))
email_spark_dataset_validate.show(truncate=False)
email_pandas_dataset = pd.read_csv('dataset\\EmailData.csv')
print(tabulate(email_pandas_dataset, headers='keys', tablefmt='psql'))
email_pandas_dataset_validate = email_pandas_dataset.copy()
email_pandas_dataset_validate['EmailFlag'] = email_pandas_dataset_validate.apply(
    lambda row: mail_validator(row['mail_id']), axis=1)
print(tabulate(email_pandas_dataset_validate, headers='keys', tablefmt='psql'))

# (5)-Read Salary dataset and generate sequential ids
salary_spark_dataset = spark.read.option("header", "true").csv("dataset\\SalaryInfo.csv")
salary_pandas_dataset = pd.read_csv('dataset\\SalaryInfo.csv')
salary_spark_dataset.show(truncate=False)
salary_spark_dataset_withId = salary_spark_dataset.withColumn("SeqID", F.row_number().over(Window.orderBy("PayBand")))
salary_spark_dataset_withId.show(truncate=False)

# (4)-Code to Convert a dataframe to list of dicts and read the same into a another dataframe with prefix of "a_" apended to the original column name
salary_as_lod = salary_spark_dataset.toJSON().collect()
listwithnewkeys = [modify_dict_keys(row) for row in salary_as_lod]
modified_salary_pandas_dataset = pd.DataFrame(listwithnewkeys)
modified_salary_spark_dataset = spark.createDataFrame(listwithnewkeys)
print(tabulate(modified_salary_pandas_dataset, headers='keys', tablefmt='psql'))
modified_salary_spark_dataset.show(truncate=False)

# (7)-Write a code snippet to do pivot table of a dataframe using pandas(assume some data) without using pandas pivot_table functions
sales_pandas_dataset = pd.read_csv("dataset\\ItemSaleInfo.csv")
print(tabulate(sales_pandas_dataset, headers='keys', tablefmt='psql'))
quarter_values = sales_pandas_dataset['Quarter'].unique()
item_values = sales_pandas_dataset['Item'].unique()
list_with_piv_sales_dict = [group_item_sales(item, sales_pandas_dataset, quarter_values) for item in item_values]
sales_pandas_dataset_pivoted = spark.createDataFrame(list_with_piv_sales_dict)
sales_pandas_dataset_pivoted.show(truncate=False)

# (8)-Write a code snippet to read a table from sqlitedb  into pandas dataframe(assume some data)
conn = sqlite3.connect('dataset\\organisation')
cursor = conn.execute("SELECT * from COMPANY")
company_dict = {}


def create_company_dictionary(row):
    return {"ID": row[0], "NAME": row[1], "AGE": row[2], "ADDRESS": row[3], "SALARY": row[4]}


company_list_dict = [create_company_dictionary(row) for row in cursor]
company_pandas_dataset = pd.DataFrame(company_list_dict)
print(tabulate(company_pandas_dataset, headers='keys', tablefmt='psql'))

# (10)-Create Spark Dataframe to populate country for cities
city_data = [("x01", "NewYork"), ("x03", "Dhaka"), ("x05", "Delhi")]
schema = StructType([
    StructField('cityId', StringType(), True),
    StructField('cityName', StringType(), True)
])
city_rdd = spark.sparkContext.parallelize(city_data)
city_spark_dataset = spark.createDataFrame(city_rdd, schema)
city_spark_dataset.show(truncate=False)
citymapping = spark.read.option("header", "true").csv("dataset\\CityCntryMapping.csv")
city_country_dataset = citymapping.join(city_spark_dataset, citymapping.City == city_spark_dataset.cityName).drop(
    "cityName")
city_country_dataset.show(truncate=False)
