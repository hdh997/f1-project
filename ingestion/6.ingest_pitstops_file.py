# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest muitiline pit_stops.json

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_funcs"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step1 - Read the Jsone file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("stop", StringType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("duration", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True)
                                      ])

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/myformula1projectdl/raw

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiLine", True).json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - rename and add new column

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed('raceId', 'race_id')\
                        .withColumnRenamed('driverId','driver_id')

# COMMAND ----------

final_df = add_ingestion_date(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Write output as parquet file

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")
