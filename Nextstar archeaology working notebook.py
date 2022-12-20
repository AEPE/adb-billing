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
# MAGIC DROP TABLE if exists importnextstar.basetype;
# MAGIC CREATE TABLE importnextstar.basetype 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/basetype";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.billinginvoiceparameter;
# MAGIC CREATE TABLE importnextstar.billinginvoiceparameter  
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/billinginvoiceparameter";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.unitofmeasure;
# MAGIC CREATE TABLE importnextstar.unitofmeasure  
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/unitofmeasure";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.accountproductservicepointdetail;
# MAGIC CREATE TABLE importnextstar.accountproductservicepointdetail  
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/accountproductservicepointdetail";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.energyusagestatus;
# MAGIC CREATE TABLE importnextstar.energyusagestatus  
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/energyusagestatus";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.energyusage;
# MAGIC CREATE TABLE importnextstar.energyusage  
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/energyusage";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.accountproduct;
# MAGIC CREATE TABLE importnextstar.accountproduct  
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/accountproduct";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.accountproductagreement;
# MAGIC CREATE TABLE importnextstar.accountproductagreement  
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/accountproductagreement";

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- select * from importnextstar.basetype
# MAGIC -- select * from importnextstar.billinginvoiceparameter
# MAGIC -- select * from importnextstar.unitofmeasure
# MAGIC -- select * from importnextstar.accountproductservicepointdetail
# MAGIC -- select * from importnextstar.energyusagestatus
# MAGIC -- select * from importnextstar.energyusage
# MAGIC -- select * from importnextstar.accountproduct
# MAGIC  select * from importnextstar.accountproductagreement

# COMMAND ----------

basetype_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.basetype"))
basetype_pandas_df_unique = ps.DataFrame(basetype_pandas_df.nunique())
basetype_pandas_df_na = ps.DataFrame(basetype_pandas_df.isna().sum())
basetype_pandas_df_unique.reset_index(inplace=True)
basetype_pandas_df_na.reset_index(inplace=True)
basetype_results = basetype_pandas_df_unique.merge(basetype_pandas_df_na, on='index')
basetype_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

billinginvoiceparameter_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.billinginvoiceparameter"))
billinginvoiceparameter_pandas_df_unique = ps.DataFrame(billinginvoiceparameter_pandas_df.nunique())
billinginvoiceparameter_pandas_df_na = ps.DataFrame(billinginvoiceparameter_pandas_df.isna().sum())
billinginvoiceparameter_pandas_df_unique.reset_index(inplace=True)
billinginvoiceparameter_pandas_df_na.reset_index(inplace=True)
billinginvoiceparameter_results = billinginvoiceparameter_pandas_df_unique.merge(billinginvoiceparameter_pandas_df_na, on='index')
billinginvoiceparameter_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

unitofmeasure_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.unitofmeasure"))
unitofmeasure_pandas_df_unique = ps.DataFrame(unitofmeasure_pandas_df.nunique())
unitofmeasure_pandas_df_na = ps.DataFrame(unitofmeasure_pandas_df.isna().sum())
unitofmeasure_pandas_df_unique.reset_index(inplace=True)
unitofmeasure_pandas_df_na.reset_index(inplace=True)
unitofmeasure_results = unitofmeasure_pandas_df_unique.merge(unitofmeasure_pandas_df_na, on='index')
unitofmeasure_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

accountproductservicepointdetail_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.accountproductservicepointdetail"))
accountproductservicepointdetail_pandas_df_unique = ps.DataFrame(accountproductservicepointdetail_pandas_df.nunique())
accountproductservicepointdetail_pandas_df_na = ps.DataFrame(accountproductservicepointdetail_pandas_df.isna().sum())
accountproductservicepointdetail_pandas_df_unique.reset_index(inplace=True)
accountproductservicepointdetail_pandas_df_na.reset_index(inplace=True)
accountproductservicepointdetail_results = accountproductservicepointdetail_pandas_df_unique.merge(accountproductservicepointdetail_pandas_df_na, on='index')
accountproductservicepointdetail_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

energyusagestatus_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.energyusagestatus"))
energyusagestatus_pandas_df_unique = ps.DataFrame(energyusagestatus_pandas_df.nunique())
energyusagestatus_pandas_df_na = ps.DataFrame(energyusagestatus_pandas_df.isna().sum())
energyusagestatus_pandas_df_unique.reset_index(inplace=True)
energyusagestatus_pandas_df_na.reset_index(inplace=True)
energyusagestatus_results = energyusagestatus_pandas_df_unique.merge(energyusagestatus_pandas_df_na, on='index')
energyusagestatus_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

energyusage_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.energyusage"))
energyusage_pandas_df_unique = ps.DataFrame(energyusage_pandas_df.nunique())
energyusage_pandas_df_na = ps.DataFrame(energyusage_pandas_df.isna().sum())
energyusage_pandas_df_unique.reset_index(inplace=True)
energyusage_pandas_df_na.reset_index(inplace=True)
energyusage_results = energyusage_pandas_df_unique.merge(energyusage_pandas_df_na, on='index')
energyusage_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)


accountproduct_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.accountproduct"))
accountproduct_pandas_df_unique = ps.DataFrame(accountproduct_pandas_df.nunique())
accountproduct_pandas_df_na = ps.DataFrame(accountproduct_pandas_df.isna().sum())
accountproduct_pandas_df_unique.reset_index(inplace=True)
accountproduct_pandas_df_na.reset_index(inplace=True)
accountproduct_results = accountproduct_pandas_df_unique.merge(accountproduct_pandas_df_na, on='index')
accountproduct_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)


accountproductagreement_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.accountproductagreement"))
accountproductagreement_pandas_df_unique = ps.DataFrame(accountproductagreement_pandas_df.nunique())
accountproductagreement_pandas_df_na = ps.DataFrame(accountproductagreement_pandas_df.isna().sum())
accountproductagreement_pandas_df_unique.reset_index(inplace=True)
accountproductagreement_pandas_df_na.reset_index(inplace=True)
accountproductagreement_results = accountproductagreement_pandas_df_unique.merge(accountproductagreement_pandas_df_na, on='index')
accountproductagreement_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

# COMMAND ----------

#display(basetype_results)
#display(billinginvoiceparameter_results)
#display(unitofmeasure_results)
#display(accountproductservicepointdetail_results)
#display(energyusagestatus_results)
#display(energyusage_results)
#display(accountproduct_results)
#display(energyusage_results)
display(accountproductagreement_results)

# COMMAND ----------

#File destination location
Path = "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/jzegarra_results.csv"
#convert pandas df to pyspark
basetype_results = basetype_results.to_spark()
billinginvoiceparameter_results = billinginvoiceparameter_results.to_spark()
unitofmeasure_results = unitofmeasure_results.to_spark()
accountproductservicepointdetail_results = accountproductservicepointdetail_results.to_spark()
energyusagestatus_results = energyusagestatus_results.to_spark()
energyusage_results = energyusage_results.to_spark()
accountproduct_results = accountproduct_results.to_spark()
accountproductagreement_results = accountproductagreement_results.to_spark()


#include the column with the table name
basetype_results = basetype_results.withColumn("table",lit("basetype"))
billinginvoiceparameter_results = billinginvoiceparameter_results.withColumn("table",lit("billinginvoiceparameter"))
unitofmeasure_results = unitofmeasure_results.withColumn("table",lit("unitofmeasure"))
accountproductservicepointdetail_results = accountproductservicepointdetail_results.withColumn("table",lit("accountproductservicepointdetail"))
energyusagestatus_results = energyusagestatus_results.withColumn("table",lit("energyusagestatus"))
energyusage_results = energyusage_results.withColumn("table",lit("energyusage"))
accountproduct_results = accountproduct_results.withColumn("table",lit("accountproduct"))
accountproductagreement_results = accountproductagreement_results.withColumn("table",lit("accountproductagreement"))
#consolidate datasets in one single object
Report = basetype_results.union(billinginvoiceparameter_results).union(unitofmeasure_results).union(accountproductservicepointdetail_results).union(energyusagestatus_results).union(energyusage_results).union(accountproduct_results).union(accountproductagreement_results)
#write to the csv file and display the report
Report.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(Path)


# COMMAND ----------

display(Report)

# COMMAND ----------

#basetype_results = basetype_results.to_spark()
basetype_results = basetype_results.withColumn("table",lit("basetype_results"))

#billinginvoiceparameter_results = billinginvoiceparameter_results.to_spark()
billinginvoiceparameter_results = billinginvoiceparameter_results.withColumn("table",lit("billinginvoiceparameter_results"))

#accountproduct_results = accountproduct_results.to_spark()
#accountproduct_results = accountproduct_results.withColumn("table",lit("accountproduct"))

Path = "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/jzegarra_results.csv"
basetype_results.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(Path)
billinginvoiceparameter_results.repartition(1).write.format("csv").mode("append").option("header", "true").save(Path)

# COMMAND ----------

#display(basetype_results)
display(billinginvoiceparameter_results)

# COMMAND ----------


