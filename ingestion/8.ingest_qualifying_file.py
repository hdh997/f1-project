# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying folder

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

qualifying_df = spark.read.schema(qualifying_schema).option('multiLine', True).json("/mnt/myformula1projectdl/raw/qualifying/")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Rename & add new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id')\
                        .withColumnRenamed('raceId', 'race_id')\
                        .withColumnRenamed('driverId', 'driver_id')\
                        .withColumnRenamed('constructorId', 'constructor_id')\
                        .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Write output as parquet file

# COMMAND ----------

final_df.write.mode('overwrite').parquet("/mnt/myformula1projectdl/processed/qualifying")
