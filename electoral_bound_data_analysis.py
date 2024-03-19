# Databricks notebook source
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
spark = SparkSession.builder.appName('electoralBondAnalysis').getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC #Party Analysis

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/party_name_amount_final_14_march.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
party_name_df = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location)

display(party_name_df)
#Temp Table
party_name_df.createOrReplaceTempView("party_name_df")

# COMMAND ----------

#Column Rename
party_name_df1 = (party_name_df
    .withColumnRenamed("Date of Encashment", "date")
    .withColumnRenamed("Name of the Political Party", "party_name")
    .withColumnRenamed("Denomination", "amount")
    .withColumn("amount", regexp_replace(trim(col("amount")), "[^0-9.]", ""))
    .withColumn("amount", col("amount").cast(FloatType()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##The unique Indian political party received political funding through electoral bounds

# COMMAND ----------

party_name_df1.select("party_name").distinct().orderBy("party_name").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##The amount received by each political party.

# COMMAND ----------

party_name_df1.select("*").groupBy("party_name").agg(sum("amount").alias("total_amount")).withColumn("amount_in_crores", col("total_amount")/10000000).orderBy(col("total_amount").desc(), col("party_name")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##The total amount received by all political parties.

# COMMAND ----------

party_name_df1.select("*").agg(sum("amount").alias("total_amount")).withColumn("amount_in_crores", col("total_amount")/10000000).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Donor Analysis

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/donor_name_amount_final_14_march.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
donor_name_df = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location)

display(donor_name_df)

# COMMAND ----------

donor_name_df1 = (donor_name_df
    .withColumnRenamed("Date of Purchase", "date")
    .withColumnRenamed("Purchaser Name", "donor_name")
    .withColumnRenamed("Denomination", "amount")
    .withColumn("amount", regexp_replace(trim(col("amount")), "[^0-9.]", ""))
    .withColumn("amount", col("amount").cast(FloatType()))
)

# COMMAND ----------

donor_name_df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Total number of unique donors

# COMMAND ----------

donor_name_df1.select("donor_name").distinct().orderBy("donor_name").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##The amount donated by each donor.

# COMMAND ----------

donor_name_df1.select("*").groupBy("donor_name").agg(sum("amount").alias("total_amount")).withColumn("amount_in_crores", col("total_amount")/10000000).orderBy(col("total_amount").desc(), col("donor_name")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

donor_name_df1.select("*").agg(sum("amount").alias("total_amount")).withColumn("amount_in_crores", col("total_amount")/10000000).display()

# COMMAND ----------


