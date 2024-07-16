# Databricks notebook source
# MAGIC %md
# MAGIC CONVERSION OF TEXT TO PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC --ALTER TABLE curate_standard.SAPCRM_Advantage_Card_membership_points_Incr ADD columns (Source_Sent_Date string, Source_Sent_Time string)
# MAGIC --describe curate_standard.SAPCRM_Advantage_Card_membership_points_Incr

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
dfSchema = StructType([ StructField("RECORD_TYPE", StringType(), True), StructField("ACCOUNT_NUMBER", StringType(), True), StructField("CUSTOMER_NUMBER", StringType(), True),StructField("MEMBERSHIP_TYPE_CODE", StringType(), True),StructField("POINTS_BALANCE", StringType(), True),StructField("POINTS_EARNED", StringType(), True),StructField("ACCOUNT_TERMINATED_DATE", StringType(), True),StructField("ACCOUNT_ENROLLED_DATE", StringType(), True),StructField("POINTS_REDEEMED", StringType(), True),StructField("POINTS_EXPIRED", StringType(), True),StructField("MEMBERSHIP_STATUS", StringType(), True),StructField("POINT_ACCOUNT_ID", StringType(), True),StructField("GEOGRAPHY_CODE", StringType(), True),StructField("LATEST_TXN_DATE", StringType(), True),StructField("CHANGED_BY", StringType(), True),StructField("CREATED_BY_CHANNEL", StringType(), True),StructField("UPDATED_BY_CHANNEL", StringType(), True)])

dfSchema1 = StructType([ StructField("RECORD_TYPE", StringType(), True), StructField("ACCOUNT_NUMBER", StringType(), True), StructField("CUSTOMER_NUMBER", StringType(), True),StructField("MEMBERSHIP_TYPE_CODE", StringType(), True),StructField("POINTS_BALANCE", StringType(), True),StructField("POINTS_EARNED", StringType(), True),StructField("ACCOUNT_TERMINATED_DATE", StringType(), True),StructField("ACCOUNT_ENROLLED_DATE", StringType(), True),StructField("POINTS_REDEEMED", StringType(), True),StructField("POINTS_EXPIRED", StringType(), True),StructField("MEMBERSHIP_STATUS", StringType(), True),StructField("POINT_ACCOUNT_ID", StringType(), True),StructField("GEOGRAPHY_CODE", StringType(), True),StructField("LATEST_TXN_DATE", StringType(), True),StructField("CHANGED_BY", StringType(), True),StructField("CREATED_BY_CHANNEL", StringType(), True),StructField("UPDATED_BY_CHANNEL", StringType(), True),
StructField("Source_Sent_Date", StringType(), True),StructField("Source_Sent_Time", StringType(), True)])

filelist1=filelist[1:-1]
print(filelist1)
for file in filelist1.split(","):
    foldername=file[1:-5]
    #print(foldername)
    dfo=foldername.split("_")
    df=spark.read.format('csv').option('delimiter',"|").schema(dfSchema).load(mountPoint+SourcePath+"/"+foldername+".TXT")
    #df.show()
    df1=df.toPandas()
    df1.insert(17,'Source_Sent_Date',dfo[4],True)
    df1.insert(18,'Source_Sent_Time',dfo[5],True)
    df1=df1[1:-1]
    df2=spark.createDataFrame(df1,schema=dfSchema1)
    #display(df2)
    df2.write.mode('append').format("parquet").save(tgtmountPoint+DestinationPath+"/"+RunDate+RunTime+"/")
    df_tnp = df.collect()[0][0]
  

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
