# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - read all dataframe

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/myformula1projectdl/processed

# COMMAND ----------

drivers_df = spark.read.parquet(f'{processed_folder_path}/drivers')\
.withColumnRenamed("number", "driver_number")\
.withColumnRenamed("name", "driver_name")\
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')\
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

constructors_df = spark.read.parquet(f'{processed_folder_path}/constructors')\
.withColumnRenamed("name", "team")

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')\
.withColumnRenamed("name", "race_name")\
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.parquet(f'{processed_folder_path}/results')\
.withColumnRenamed("time", "race_time")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Join all DF

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id, "inner")\
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner")\
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step4 - Select wanted fields & add current time

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points","position")\
.withColumn("created_time", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step5 - Write output to performance storage

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{performance_folder_path}/race_results")
