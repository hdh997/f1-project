# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce constructor standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{performance_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

team_standings = race_results_df.groupBy("race_year", "team")\
                .agg(sum('points').alias("total_points"), count(when(col('position')== 1, True)).alias('wins'))

# COMMAND ----------

display(team_standings.filter("race_year = 2019"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

# COMMAND ----------

team_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc('wins'))
final_df = team_standings.withColumn("rank", rank().over(team_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable('f1_performance.team_standings')
