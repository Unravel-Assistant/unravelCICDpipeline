# Databricks notebook source
# MAGIC %md
# MAGIC CONVERSION OF TEXT TO PARQUET

# COMMAND ----------

# MAGIC %sql 
# MAGIC --ALTER TABLE curate_standard.SAPCOE_SAPCRM_BUK_SAPCRM_Advantage_Card_customer_child ADD columns (Source_Sent_Date string, Source_Sent_Time string);
# MAGIC --describe curate_standard.SAPCOE_SAPCRM_BUK_SAPCRM_Advantage_Card_customer_child 
# MAGIC --ALTER TABLE delta.`/mnt/idf-curatestandard/SAPCOE/SAPCRM/BUK_SAPCRM_Advantage_Card_customer_child_Incr/BackUp/` ADD columns (Source_Sent_Date string, Source_Sent_Time string);
# MAGIC --describe delta.`/mnt/idf-curatestandard/SAPCOE/SAPCRM/BUK_SAPCRM_Advantage_Card_customer_child_Incr/BackUp/`

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text FileList default "";
# MAGIC create widget text SourcePath default "";
# MAGIC create widget text DestinationPath default "";
# MAGIC create widget text RunDate default "";
# MAGIC create widget text RunTime default "";

# COMMAND ----------

import os
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import *


mountPoint='/mnt/idf-cleansed/'
tgtmountPoint='/mnt/idf-curatestage/'
first_row_is_header = "true"
delimiter = "|"
filelist=dbutils.widgets.get("FileList")
SourcePath=dbutils.widgets.get("SourcePath")
RunDate=dbutils.widgets.get("RunDate")
RunTime=dbutils.widgets.get("RunTime")
DestinationPath=dbutils.widgets.get("DestinationPath")
SourceFileCount=len(filelist.split(","))
OutputTableList=[]
dfSchema = StructType([ StructField("RECORD_TYPE", StringType(), True), StructField("CUSTOMER_NUMBER", StringType(), True), StructField("CHILD_NUMBER", StringType(), True),StructField("DATE_TYPE", StringType(), True),StructField("CHILD_DOB", StringType(), True),StructField("CHILD_FORENAME", StringType(), True),StructField("CHILD_GENDER", StringType(), True),StructField("IUD_FLAG", StringType(), True),StructField("CREATE_USER", StringType(), True),StructField("CHANGE_USER", StringType(), True),StructField("CREATED_BY_CHANNEL", StringType(), True),StructField("UPDATED_BY_CHANNEL", StringType(), True)])
dfSchema1 = StructType([ StructField("RECORD_TYPE", StringType(), True), StructField("CUSTOMER_NUMBER", StringType(), True), StructField("CHILD_NUMBER", StringType(), True),StructField("DATE_TYPE", StringType(), True),StructField("CHILD_DOB", StringType(), True),StructField("CHILD_FORENAME", StringType(), True),StructField("CHILD_GENDER", StringType(), True),StructField("IUD_FLAG", StringType(), True),StructField("CREATE_USER", StringType(), True),StructField("CHANGE_USER", StringType(), True),StructField("CREATED_BY_CHANNEL", StringType(), True),StructField("UPDATED_BY_CHANNEL", StringType(), True),StructField("Source_Sent_Date", StringType(), True),StructField("Source_Sent_Time", StringType(), True)])

filelist1=filelist[1:-1]
print(filelist1)
for file in filelist1.split(","):
    foldername=file[1:-5]
    print(foldername)
    dfo=foldername.split("_")
    df=spark.read.format('csv').option('delimiter',"|").schema(dfSchema).load(mountPoint+SourcePath+"/"+foldername+".TXT")
    #df.show()
    df1=df.toPandas()
    df1.insert(12,'Source_Sent_Date',dfo[4],True)
    df1.insert(13,'Source_Sent_Time',dfo[5],True)
    df1=df1[1:-1]
    #display(df1)
    df2=spark.createDataFrame(df1,schema=dfSchema1)
    #display(df2)
    df2.write.mode('append').format("parquet").save(tgtmountPoint+DestinationPath+"/"+RunDate+RunTime+"/")
    deets_df = df.collect()[0][0]
  

# COMMAND ----------

import os
from os import listdir
filelist1=filelist[1:-1]
for file1 in filelist1.split(","):
    foldername=file1[1:-5]
    OutputTableList.append(foldername)
    DestinationTableCount=len(OutputTableList)

# COMMAND ----------

dbutils.notebook.exit({"SourceCount":SourceFileCount,"DestinationCount":DestinationTableCount,"DestinationTableList":OutputTableList})
