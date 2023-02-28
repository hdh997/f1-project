# Databricks notebook source
# MAGIC %md
# MAGIC ### Check files

# COMMAND ----------

spark.read.json('/mnt/myformula1projectdl/raw/2021-03-21/results.json').createOrReplaceTempView('results_cutover')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId, COUNT(1)
# MAGIC   FROM results_cutover
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC

# COMMAND ----------

spark.read.json('/mnt/myformula1projectdl/raw/2021-04-18/results.json').createOrReplaceTempView('results_w2')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId, COUNT(1)
# MAGIC   FROM results_w2
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ingest results.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_funcs"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

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

results_df = spark.read.schema(results_schema).json(f'{raw_folder_path}/{v_file_date}/results.json')

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
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))

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
# MAGIC #### Deduplicate

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step4 - Write output as parquet file and partition by race ID

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1

# COMMAND ----------

# #prevent same records written
# for race_id_list in results_final_df.select('race_id').distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")): #check if table exists
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode('append').partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results;

# COMMAND ----------

# overwrite_partition(results_final_df,'f1_processed', 'results', 'race_id')

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# results_final_df = results_final_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order",
#                                           "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed",
#                                           "data_source", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_final_df.write.mode('overwrite').partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC With Delta

# COMMAND ----------

merge_cond = 'tgt.result_id = src.result_id AND tgt.race_id = src.race_id'

merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_cond, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Done!")
