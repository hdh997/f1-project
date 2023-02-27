# Databricks notebook source
#Add date column
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_col(input_df, partition_col):
    col_list = []
    for name in input_df.schema.names:
        if name != partition_col:
            col_list.append(name)
    col_list.append(partition_col)

    output_df =  input_df.select(col_list)
    
    return output_df

# COMMAND ----------

# MAGIC %md
# MAGIC Use for parquet

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_col):
    output_df = re_arrange_col(input_df, partition_col)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode('overwrite').partitionBy(partition_col).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_col_to_ls(input_df, col_name):
    df_row_ls = input_df.filter(col_name)\
                .distinct()\
                .collect()
    col_values_ls = [row[col_name] for row in df_row_ls]
    return col_values_ls

# COMMAND ----------

# MAGIC %md
# MAGIC Use for Delta

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_cond, partition_col):

    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruing","true")

    from delta.tables import DeltaTable

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f'{folder_path}/{table_name}')
        deltaTable.alias('tgt').merge(
            input_df.alias('src'),
            merge_cond)\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        input_df.write.mode('overwrite').partitionBy(partition_col).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------


