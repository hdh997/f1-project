# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_funcs"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step1 - Read the folder using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

lap_times_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("lap", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True)
                                      ])

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/myformula1projectdl/raw

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - rename and add new column

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed('raceId', 'race_id')\
                        .withColumnRenamed('driverId','driver_id')

# COMMAND ----------

final_df = add_ingestion_date(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Write output as parquet file

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")
