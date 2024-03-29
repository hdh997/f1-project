# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest drivers.json file

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
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
                                ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", StringType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(),True),
                                    StructField("nationality", StringType(),True),
                                    StructField("url", StringType(),True)
                                   ])

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/myformula1projectdl/raw

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f'{raw_folder_path}/{v_file_date}/drivers.json')

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and ad new columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

drivers_with_column_df = drivers_df.withColumnRenamed('driverId', 'driver_id')\
                                    .withColumnRenamed('driverRef', 'driver_ref')\
                                    .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))\
                                    .withColumn("data_source", lit(v_data_source))\
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

drivers_with_column_df = add_ingestion_date(drivers_with_column_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Drop unwanted column

# COMMAND ----------

drivers_final_df = drivers_with_column_df.drop(drivers_with_column_df.url)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step4 - Write output as parquet file

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Done!")
