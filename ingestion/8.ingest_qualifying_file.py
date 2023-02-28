# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying folder

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_funcs"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step1 - Read the folder using the spark dataframe reader 

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField('qualifyId', IntegerType(), False),
                                       StructField('raceId', IntegerType(), True),
                                       StructField('driverId', IntegerType(), True),
                                       StructField('constructorId', IntegerType(), True),
                                       StructField('number', IntegerType(), True),
                                       StructField('position', IntegerType(), True),
                                       StructField('q1', StringType(), True),
                                       StructField('q2', StringType(), True),
                                       StructField('q3', StringType(), True)
])

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/myformula1projectdl/raw

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option('multiLine', True).json(f"{raw_folder_path}/{v_file_date}/qualifying/")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Rename & add new column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id')\
                        .withColumnRenamed('raceId', 'race_id')\
                        .withColumnRenamed('driverId', 'driver_id')\
                        .withColumnRenamed('constructorId', 'constructor_id')\
                        .withColumn('data_source', lit(v_data_source))\
                        .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

final_df = add_ingestion_date(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Write output as parquet file

# COMMAND ----------

# final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# overwrite_partition(final_df,'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_cond = 'tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id'

merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_cond, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id;

# COMMAND ----------

dbutils.notebook.exit("Done!")
