# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce driver standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_funcs"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find race years for which the data is to be processed

# COMMAND ----------

# race_results_ls = spark.read.parquet(f"{performance_folder_path}/race_results")\
# .filter(f"file_date = '{v_file_date}'")\
# .select("race_year")\
# .distinct()\
# .collect()

# COMMAND ----------

race_results_ls = spark.read.format("delta").load(f"{performance_folder_path}/race_results")\
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

display(race_results_ls)

# COMMAND ----------

race_year_list = df_col_to_ls(race_results_ls, "race_year")

# COMMAND ----------

# race_year_list = []
# for race_year in race_results_ls:
#     race_year_list.append(race_year.race_year)
# print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{performance_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

dtiver_standings_df = race_results_df.groupBy("race_year","driver_name","driver_nationality")\
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

#final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_performance.driver_standings")

#overwrite_partition(final_df,'f1_performance', 'driver_standings', 'race_year')

# COMMAND ----------

merge_cond = 'tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year'
 
merge_delta_data(final_df, 'f1_performance', 'driver_standings', performance_folder_path, merge_cond, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM f1_performance.driver_standings
