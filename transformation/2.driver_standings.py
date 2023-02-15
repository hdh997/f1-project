# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce driver standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{performance_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

dtiver_standings_df = race_results_df.groupBy("race_year","driver_name","driver_nationality","team")\
                                    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias('wins'))
                                    

# COMMAND ----------

display(dtiver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = dtiver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

final_df.filter("race_year = 2020").show()

# COMMAND ----------

final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_performance.driver_standings")
