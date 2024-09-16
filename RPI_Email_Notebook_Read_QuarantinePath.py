# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists RPI_Reference_DB.Curation_BadRowCount(BatchID string,FeedID string,EntityName string,BadRowCount int)
# MAGIC using delta 
# MAGIC partitioned by (BatchID)
# MAGIC location '/mnt/idf-reports/RPI_Reference/Curation/BadRowCount/';

# COMMAND ----------

from pyspark.sql.functions import col
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import os
import requests
from requests.auth import HTTPDigestAuth
import json
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql import Row

dbutils.widgets.text("config_file_name", '', '')
config_file_path = dbutils.widgets.get('config_file_name') 

dbutils.widgets.text("bad_row_count", '', '')
bad_row_count = dbutils.widgets.get('bad_row_count')

dbutils.widgets.text("FeedID", '', '')
FeedID = dbutils.widgets.get('FeedID')

dbutils.widgets.text("EntityName", '', '')
EntityName = dbutils.widgets.get('EntityName')

dbutils.widgets.text("BatchID", '', '')
BatchID = dbutils.widgets.get('BatchID')

dbutils.widgets.text("file_name", '', '')
file_name = dbutils.widgets.get('file_name')

dbutils.widgets.text("Pipeline_Process_Name", '', '')
Pipeline_Process_Name = dbutils.widgets.get('Pipeline_Process_Name')

status = 'Failure'

if ((bad_row_count.isnumeric()) and (int(bad_row_count) >0)):
    filepath = '/mnt/idf-config/' + config_file_path
    df=spark.read.format('json').option("multiline","true").option('inferSchema','true').load(filepath,header =True).select("quarantine_path")
    df2=df.collect()[0][0]
    error_details = "The bad row count is :" + str(bad_row_count)+"\n" + "The quarantine path location is :" +str(df2)
    print(error_details)
    LogicAppURL = "https://prod-58.northeurope.logic.azure.com:443/workflows/dc2ac967bf074d4a854d84f1761c08d2/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=h6AYw5lWMnZLRdcOfx4axp2bAllULinZDFkwsLBExhk"
    data ={"FileName":file_name,
         "ErrorDetails":error_details,
         "status":status,
         "Pipeline_Process_Name":Pipeline_Process_Name}
    myResponse = requests.post(LogicAppURL,params = data)
    if ((FeedID=='IF_00461')|(FeedID=='IF_00462')|(FeedID=='IF_00463')):
        FeedID='IF_00461,IF_00462,IF_00463'
        updatedvalue=[(BatchID,FeedID,EntityName,bad_row_count)]
    else:
        updatedvalue=[(BatchID,FeedID,EntityName,bad_row_count)]
    updatedDF=spark.createDataFrame(updatedvalue,schema=['BatchID','FeedID','EntityName','BadRowCount']).withColumn("BadRowCount",col("BadRowCount").cast('integer'))
    updatedDF.write.format("delta").mode("append").partitionBy("BatchID").saveAsTable("RPI_Reference_DB.Curation_BadRowCount")
else:
    dbutils.notebook.exit ('stop')
    
