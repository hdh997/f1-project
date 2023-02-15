# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest results.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_funcs"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - Read JSON file using spark dataframe reader

# COMMAND ----------

results_schema = 'resultId INT, raceId INT, driverId INT, constructorId INT, number INT, grid INT, position INT, positionText STRING, positionOrder INT, points FLOAT, laps INT, time STRING, milliseconds INT, fastestLap INT, rank INT, fastestLapTime STRING, fastestLapSpeed STRING, statusId INT'

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls mnt/myformula1projectdl/raw

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f'{raw_folder_path}/results.json')

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id")\
.withColumnRenamed('raceId', 'race_id')\
.withColumnRenamed('driverId', 'driver_id')\
.withColumnRenamed('constructorId', 'constructor_id')\
.withColumnRenamed('positionText', 'position_text')\
.withColumnRenamed('positionOrder', 'position_order')\
.withColumnRenamed('fastestLap', 'fastest_lap')\
.withColumnRenamed('fastestLapTime', 'fastest_lap_time')\
.withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')\
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

results_renamed_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Drop unwanted column

# COMMAND ----------

results_final_df = results_renamed_df.drop(results_renamed_df.statusId)

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step4 - Write output as parquet file and partition by race ID

# COMMAND ----------

results_final_df.write.mode('overwrite').partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

dbutils.notebook.exit("Done!")
