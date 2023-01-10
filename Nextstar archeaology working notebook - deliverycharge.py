# Databricks notebook source
import pyspark.pandas as ps
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, DoubleType, DateType, DecimalType , TimestampType

# COMMAND ----------

storage_account_access_key = "yNWpvxnfTgCHt1mUlj8YPvCHbv4a/c5vmQQ4kcM4I0EC+VQDTxF7ffQ4bS340um02Z+mRYtMVavZ+AStLnlGyw=="

spark.conf.set(
  "fs.azure.account.key.stgbillingpoc.blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --DROP DATABASE importnextstar CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS importnextstar;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverycharge;
# MAGIC CREATE TABLE importnextstar.deliverycharge 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverycharge";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverychargecodedescription;
# MAGIC CREATE TABLE importnextstar.deliverychargecodedescription 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverychargecodedescription";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverychargedetail;
# MAGIC CREATE TABLE importnextstar.deliverychargedetail
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverychargedetail";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverychargeincludedetail;
# MAGIC CREATE TABLE importnextstar.deliverychargeincludedetail 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverychargeincludedetail";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverychargeitemtype;
# MAGIC CREATE TABLE importnextstar.deliverychargeitemtype 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverychargeitemtype";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverychargemessage;
# MAGIC CREATE TABLE importnextstar.deliverychargemessage 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverychargemessage";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverychargestatus;
# MAGIC CREATE TABLE importnextstar.deliverychargestatus 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverychargestatus";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverychargetemplate;
# MAGIC CREATE TABLE importnextstar.deliverychargetemplate 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverychargetemplate";

# COMMAND ----------

deliverycharge = sqlContext.sql("select * from importnextstar.deliverycharge")
deliverycharge_pandas_df = ps.DataFrame(deliverycharge)
deliverycharge_pandas_df_unique = ps.DataFrame(deliverycharge_pandas_df.nunique())
deliverycharge_pandas_df_na = ps.DataFrame(deliverycharge_pandas_df.isna().sum())
deliverycharge_pandas_df_unique.reset_index(inplace=True)
deliverycharge_pandas_df_na.reset_index(inplace=True)
deliverycharge_results = deliverycharge_pandas_df_unique.merge(deliverycharge_pandas_df_na, on='index')
deliverycharge_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverychargecodedescription = sqlContext.sql("select * from importnextstar.deliverychargecodedescription")
deliverychargecodedescription_pandas_df = ps.DataFrame(deliverychargecodedescription)
deliverychargecodedescription_pandas_df_unique = ps.DataFrame(deliverychargecodedescription_pandas_df.nunique())
deliverychargecodedescription_pandas_df_na = ps.DataFrame(deliverychargecodedescription_pandas_df.isna().sum())
deliverychargecodedescription_pandas_df_unique.reset_index(inplace=True)
deliverychargecodedescription_pandas_df_na.reset_index(inplace=True)
deliverychargecodedescription_results = deliverychargecodedescription_pandas_df_unique.merge(deliverychargecodedescription_pandas_df_na, on='index')
deliverychargecodedescription_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverychargedetail = sqlContext.sql("select * from importnextstar.deliverychargedetail")
deliverychargedetail_pandas_df = ps.DataFrame(deliverychargedetail)
deliverychargedetail_pandas_df_unique = ps.DataFrame(deliverychargedetail_pandas_df.nunique())
deliverychargedetail_pandas_df_na = ps.DataFrame(deliverychargedetail_pandas_df.isna().sum())
deliverychargedetail_pandas_df_unique.reset_index(inplace=True)
deliverychargedetail_pandas_df_na.reset_index(inplace=True)
deliverychargedetail_results = deliverychargedetail_pandas_df_unique.merge(deliverychargedetail_pandas_df_na, on='index')
deliverychargedetail_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverychargeincludedetail = sqlContext.sql("select * from importnextstar.deliverychargeincludedetail")
deliverychargeincludedetail_pandas_df = ps.DataFrame(deliverychargeincludedetail)
deliverychargeincludedetail_pandas_df_unique = ps.DataFrame(deliverychargeincludedetail_pandas_df.nunique())
deliverychargeincludedetail_pandas_df_na = ps.DataFrame(deliverychargeincludedetail_pandas_df.isna().sum())
deliverychargeincludedetail_pandas_df_unique.reset_index(inplace=True)
deliverychargeincludedetail_pandas_df_na.reset_index(inplace=True)
deliverychargeincludedetail_results = deliverychargeincludedetail_pandas_df_unique.merge(deliverychargeincludedetail_pandas_df_na, on='index')
deliverychargeincludedetail_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverychargeitemtype = sqlContext.sql("select * from importnextstar.deliverychargeitemtype")
deliverychargeitemtype_pandas_df = ps.DataFrame(deliverychargeitemtype)
deliverychargeitemtype_pandas_df_unique = ps.DataFrame(deliverychargeitemtype_pandas_df.nunique())
deliverychargeitemtype_pandas_df_na = ps.DataFrame(deliverychargeitemtype_pandas_df.isna().sum())
deliverychargeitemtype_pandas_df_unique.reset_index(inplace=True)
deliverychargeitemtype_pandas_df_na.reset_index(inplace=True)
deliverychargeitemtype_results = deliverychargeitemtype_pandas_df_unique.merge(deliverychargeitemtype_pandas_df_na, on='index')
deliverychargeitemtype_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverychargemessage = sqlContext.sql("select * from importnextstar.deliverychargemessage")
deliverychargemessage_pandas_df = ps.DataFrame(deliverychargemessage)
deliverychargemessage_pandas_df_unique = ps.DataFrame(deliverychargemessage_pandas_df.nunique())
deliverychargemessage_pandas_df_na = ps.DataFrame(deliverychargemessage_pandas_df.isna().sum())
deliverychargemessage_pandas_df_unique.reset_index(inplace=True)
deliverychargemessage_pandas_df_na.reset_index(inplace=True)
deliverychargemessage_results = deliverychargemessage_pandas_df_unique.merge(deliverychargemessage_pandas_df_na, on='index')
deliverychargemessage_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverychargestatus = sqlContext.sql("select * from importnextstar.deliverychargestatus")
deliverychargestatus_pandas_df = ps.DataFrame(deliverychargestatus)
deliverychargestatus_pandas_df_unique = ps.DataFrame(deliverychargestatus_pandas_df.nunique())
deliverychargestatus_pandas_df_na = ps.DataFrame(deliverychargestatus_pandas_df.isna().sum())
deliverychargestatus_pandas_df_unique.reset_index(inplace=True)
deliverychargestatus_pandas_df_na.reset_index(inplace=True)
deliverychargestatus_results = deliverychargestatus_pandas_df_unique.merge(deliverychargestatus_pandas_df_na, on='index')
deliverychargestatus_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverychargetemplate = sqlContext.sql("select * from importnextstar.deliverychargetemplate")
deliverychargetemplate_pandas_df = ps.DataFrame(deliverychargetemplate)
deliverychargetemplate_pandas_df_unique = ps.DataFrame(deliverychargetemplate_pandas_df.nunique())
deliverychargetemplate_pandas_df_na = ps.DataFrame(deliverychargetemplate_pandas_df.isna().sum())
deliverychargetemplate_pandas_df_unique.reset_index(inplace=True)
deliverychargetemplate_pandas_df_na.reset_index(inplace=True)
deliverychargetemplate_results = deliverychargetemplate_pandas_df_unique.merge(deliverychargetemplate_pandas_df_na, on='index')
deliverychargetemplate_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

#pending to include a tablename col 

# COMMAND ----------

deliverycharge_results = deliverycharge_results.to_spark()
deliverycharge_results = deliverycharge_results.withColumn("table",lit("deliverycharge"))

deliverychargecodedescription_results = deliverychargecodedescription_results.to_spark()
deliverychargecodedescription_results = deliverychargecodedescription_results.withColumn("table",lit("deliverychargecodedescription"))

deliverychargedetail_results = deliverychargedetail_results.to_spark()
deliverychargedetail_results = deliverychargedetail_results.withColumn("table",lit("deliverychargedetail"))

deliverychargeincludedetail_results = deliverychargeincludedetail_results.to_spark()
deliverychargeincludedetail_results = deliverychargeincludedetail_results.withColumn("table",lit("deliverychargeincludedetail"))

deliverychargeitemtype_results = deliverychargeitemtype_results.to_spark()
deliverychargeitemtype_results = deliverychargeitemtype_results.withColumn("table",lit("deliverychargeitemtype"))

deliverychargemessage_results = deliverychargemessage_results.to_spark()
deliverychargemessage_results = deliverychargemessage_results.withColumn("table",lit("deliverychargemessage"))

deliverychargestatus_results = deliverychargestatus_results.to_spark()
deliverychargestatus_results = deliverychargestatus_results.withColumn("table",lit("deliverychargestatus"))

deliverychargetemplate_results = deliverychargetemplate_results.to_spark()
deliverychargetemplate_results = deliverychargetemplate_results.withColumn("table",lit("deliverychargetemplate"))

Path = "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/unified_results.csv"

#consolidate datasets in one single object
Report = deliverycharge_results.union(deliverychargecodedescription_results).union(deliverychargedetail_results).union(deliverychargeincludedetail_results).union(deliverychargeitemtype_results).union(deliverychargemessage_results).union(deliverychargestatus_results).union(deliverychargetemplate_results)

#write to the csv file and display the report
Report.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(Path)
display(Report)

