# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### 1. Write data to delta lake (managed table)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS f1_test
# MAGIC LOCATION '/mnt/myformula1projectdl/demo'

# COMMAND ----------

result_df = spark.read.option('inferSchema', True)\
            .json('/mnt/myformula1projectdl/raw/2021-03-28/results.json')

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").saveAsTable("f1_test.result_demo_managaed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_test.demo

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2. Write data to delta lake (External table)

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").save("/mnt/myformula1projectdl/demo/result_demo_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_test.result_demo_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/myformula1projectdl/demo/result_demo_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_test.result_demo_external

# COMMAND ----------

result_external_df = spark.read.format("delta").load("/mnt/myformula1projectdl/demo/result_demo_external")

# COMMAND ----------

display(result_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ####3. Partition

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").partitionBy('constructorId').saveAsTable("f1_test.result_demo_partition")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ####4.Update Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM f1_test.result_demo_managaed

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_test.result_demo_managaed
# MAGIC   SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM f1_test.result_demo_managaed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/myformula1projectdl/demo/result_demo_managaed")

deltaTable.update("position <= 10", {"points": "21 - position"} )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM f1_test.result_demo_managaed

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 5. Delete from table

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_test. result_demo_managaed
# MAGIC WHERE position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_test.result_demo_managaed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/myformula1projectdl/demo/result_demo_managaed")

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_test.result_demo_managaed

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 6.Upsert ising merge

# COMMAND ----------

drivers_day1_df = spark.read\
.option("inferSchema", True)\
.json('/mnt/myformula1projectdl/raw/2021-03-28/drivers.json')\
.filter("driverId <= 10")\
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_daty1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read\
.option('inferSchema', True)\
.json('/mnt/myformula1projectdl/raw/2021-03-28/drivers.json')\
.filter('driverId BETWEEN 6 AND 15')\
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read\
.option('inferSchema', True)\
.json('/mnt/myformula1projectdl/raw/2021-03-28/drivers.json')\
.filter('driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20')\
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_test.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC DAY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_test.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.createdDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES(driverId, dob, forename, surname, CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM f1_test.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC DAY 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_test.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.updatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES(driverId, dob, forename, surname, CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM f1_test.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC DAY 3

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTablePeople = DeltaTable.forPath(spark, '/mnt/myformula1projectdl/demo/drivers_merge')

deltaTablePeople.alias('tgt') \
  .merge(
    drivers_day3_df.alias('udp'),
    'tgt.driverId = udp.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "udp.dob",
      "forename": "udp.forename",
      "surname": "udp.surname",
      "updatedDate": "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "udp.driverId",
      "dob": "udp.dob",
      "forename": "udp.forename",
      "surname": "udp.surname",
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM f1_test.drivers_merge;

# COMMAND ----------


