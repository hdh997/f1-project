# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce constructor standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_funcs"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find reace year for which data to be processed

# COMMAND ----------

race_results_ls = spark.read.parquet(f"{performance_folder_path}/race_results")\
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_col_to_ls(race_results_ls, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f"{performance_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count

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

# final_df.write.mode("overwrite").format("parquet").saveAsTable('f1_performance.team_standings')

overwrite_partition(final_df,'f1_performance', 'team_standings', 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM f1_performance.team_standings
