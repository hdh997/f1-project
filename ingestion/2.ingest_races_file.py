# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_funcs"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step1 - read csv file to dataframe

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/myformula1projectdl/raw

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
]) 

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Select only the required columns & rename columns

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

races_rename_select_df = races_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("date"), col("time"))

# COMMAND ----------

display(races_rename_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Concat time and date & add ingested date

# COMMAND ----------

races_semi_df = races_rename_select_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

races_semi_df = add_ingestion_date(races_semi_df)

# COMMAND ----------

display(races_semi_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step4 - drop time and date columns

# COMMAND ----------

races_final_df = races_semi_df.drop("date", "time")

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step5 - write to processed folder as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/myformula1projectdl/processed

# COMMAND ----------

dbutils.notebook.exit("Done!")
