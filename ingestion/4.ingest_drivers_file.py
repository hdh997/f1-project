# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest drivers.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_funcs"

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

drivers_df = spark.read.schema(drivers_schema).json(f'{raw_folder_path}/drivers.json')

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
                                    .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))

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

drivers_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/drivers')

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/drivers'))
