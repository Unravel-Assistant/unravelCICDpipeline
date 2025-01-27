# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC Get the parameters

# COMMAND ----------

dbutils.widgets.text("EntityName", "","")
EntityName = dbutils.widgets.get("EntityName")

dbutils.widgets.text("CurateADLS_LPT", "","")
CurateADLS_LPT = dbutils.widgets.get("CurateADLS_LPT")

dbutils.widgets.text("CurcccccateSynapse_LPT", "","")
CurateSynapse_LPT = dbutils.widgets.get("CurateSynapse_LPT")

dbutils.widgets.text("BatchID", "","")
BatchID = dbutils.widgets.get("BatchID")

dbutils.widgets.text("Process", "","")
Process = dbutils.widgets.get("Process")

# COMMAND ----------

# MAGIC %md
# MAGIC Create database and table for tracking the latest processing time and batch id for each source

# COMMAND ----------

# MAGIC %sql
# MAGIC create Database if not exists RPI_Reference_DB;
# MAGIC
# MAGIC create table if not exists RPI_Reference_DB.Curation_WatermarkTable (EntityName string, BatchID string, CurateADLS_LPT timestamp, CurateSynapse_LPT timestamp) 
# MAGIC using delta 
# MAGIC partitioned by (EntityName)
# MAGIC location '/mnt/idf-reports/RPI_Reference/Curation/WatermarkTable/';

# COMMAND ----------

# MAGIC %md
# MAGIC create temperory data frame with latest data. Values will be received from pipeline parameters.

# COMMAND ----------

updatedvalue=[(EntityName,BatchID,CurateADLS_LPT,CurateSynapse_LPT)]
updatedDF=spark.createDataFrame(updatedvalue,schema=['EntityName','BatchID','CurateADLS_LPT','CurateSynapse_LPT']).withColumn("CurateADLS_LPT",col("CurateADLS_LPT").cast('Timestamp')).withColumn("CurateSynapse_LPT",col("CurateSynapse_LPT").cast('Timestamp'))

display(updatedDF)

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, "/mnt/idf-reports/RPI_Reference/Curation/WatermarkTable/")

if Process=="CurateADLS":
  deltaTable.alias("events").merge(
      updatedDF.alias("updates"),
      "events.EntityName = updates.EntityName and events.BatchID = updates.BatchID and events.EntityName ='"+EntityName+"'") \
    .whenMatchedUpdate(set = { "CurateADLS_LPT" : "updates.CurateADLS_LPT"} ) \
    .whenNotMatchedInsert(values =
      {
        "EntityName": "updates.EntityName",
        "BatchID": "updates.BatchID",
        "CurateADLS_LPT": "updates.CurateADLS_LPT"
      }
    ).execute()
elif Process=="CurateSynapse":
  deltaTable.alias("events").merge(
      updatedDF.alias("updates"),
      "events.EntityName = updates.EntityName and events.BatchID = updates.BatchID and events.EntityName ='"+EntityName+"'") \
    .whenMatchedUpdate(set = { "CurateSynapse_LPT" : "updates.CurateSynapse_LPT"} ) \
    .whenNotMatchedInsert(values =
      {
        "EntityName": "updates.EntityName",
        "BatchID": "updates.BatchID",
        "CurateSynapse_LPT": "updates.CurateSynapse_LPT"
      }
    ).execute()

# COMMAND ----------

# MAGIC %md
# MAGIC Save the data to delta lake table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from RPI_Reference_DB.Curation_WatermarkTable
