# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest muitiline pit_stops.json

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

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiLine", True).json("/mnt/myformula1projectdl/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - rename and add new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed('raceId', 'race_id')\
                        .withColumnRenamed('driverId','driver_id')\
                        .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Write output as parquet file

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/myformula1projectdl/processed/pit_stops")
