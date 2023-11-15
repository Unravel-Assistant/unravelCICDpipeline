# Databricks notebook source
# MAGIC %md
# MAGIC CONVERSION OF TEXT TO PARQUET

# COMMAND ----------

# MAGIC %sql 
# MAGIC --ALTER TABLE curate_standard.SAPCRM_Advantage_Card_membership_activity_Incr ADD columns (Source_Sent_Date string, Source_Sent_Time string);
# MAGIC --describe curate_standard.SAPCRM_Advantage_Card_membership_activity_Incr 
# MAGIC --ALTER TABLE delta.`/mnt/idf-curatestandard/SAPCOE/SAPCRM/BUK_SAPCRM_Advantage_Card_membership_activity_Incr/BackUp` ADD columns (Source_Sent_Date string, Source_Sent_Time string);
# MAGIC --describe delta.`/mnt/idf-curatestandard/SAPCOE/SAPCRM/BUK_SAPCRM_Advantage_Card_membership_activity_Incr/BackUp`

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
dfSchema = StructType([ StructField("RECORD_TYPE", StringType(), True), StructField("ACCOUNT_NUMBER", StringType(), True), StructField("MEMBER_ACTIVITY_ID", StringType(), True),StructField("POINT_TRANSACTION_DATE", StringType(), True),StructField("POINT_TRANSACTION_TIME", StringType(), True),StructField("CUSTOMER_NUMBER", StringType(), True),StructField("POINTS_REQUESTED", StringType(), True),StructField("POINTS_BALANCE", StringType(), True),StructField("QUALIFIYING_SPEND", StringType(), True),StructField("TOTAL_SPEND", StringType(), True),StructField("POINT_ACCOUNT_ID", StringType(), True),StructField("CURRENCY_CODE", StringType(), True),StructField("EXTERNAL_REF_NO", StringType(), True),StructField("THIRD_PARTY_ID", StringType(), True),StructField("TILL_TXN_TIMESTAMP", StringType(), True),StructField("STORE_NUMBER", StringType(), True),StructField("TERMINAL_ID", StringType(), True),StructField("CHANNEL_ID", StringType(), True),StructField("ACTIVITY_TYPE", StringType(), True),StructField("CATEGORY", StringType(), True),StructField("OPERATOR_ID", StringType(), True),StructField("MANUAL_FLAG", StringType(), True),StructField("CARD_NUMBER", StringType(), True),StructField("RECEIPT_NUMBER", StringType(), True),StructField("COUNTRY_CODE", StringType(), True),StructField("ADCARD_PRESENTATION_METHOD", StringType(), True),StructField("CHANGED_BY", StringType(), True),StructField("OFFLINE_FLAG", StringType(), True),StructField("MEMBER_ACTIVITY_STATUS", StringType(), True)])

dfSchema1 = StructType([ StructField("RECORD_TYPE", StringType(), True),
StructField("ACCOUNT_NUMBER", StringType(), True),
StructField("MEMBER_ACTIVITY_ID", StringType(), True),
StructField("POINT_TRANSACTION_DATE", StringType(), True),
StructField("POINT_TRANSACTION_TIME", StringType(), True),
StructField("CUSTOMER_NUMBER", StringType(), True),
StructField("POINTS_REQUESTED", StringType(), True),
StructField("POINTS_BALANCE", StringType(), True),
StructField("QUALIFIYING_SPEND", StringType(), True),
StructField("TOTAL_SPEND", StringType(), True),
StructField("POINT_ACCOUNT_ID", StringType(), True),
StructField("CURRENCY_CODE", StringType(), True),
StructField("EXTERNAL_REF_NO", StringType(), True),
StructField("THIRD_PARTY_ID", StringType(), True),
StructField("TILL_TXN_TIMESTAMP", StringType(), True),
StructField("STORE_NUMBER", StringType(), True),
StructField("TERMINAL_ID", StringType(), True),
StructField("CHANNEL_ID", StringType(), True),
StructField("ACTIVITY_TYPE", StringType(), True),
StructField("CATEGORY", StringType(), True),
StructField("OPERATOR_ID", StringType(), True),
StructField("MANUAL_FLAG", StringType(), True),
StructField("CARD_NUMBER", StringType(), True),
StructField("RECEIPT_NUMBER", StringType(), True),
StructField("COUNTRY_CODE", StringType(), True),
StructField("ADCARD_PRESENTATION_METHOD", StringType(), True),
StructField("CHANGED_BY", StringType(), True),
StructField("OFFLINE_FLAG", StringType(), True),
StructField("MEMBER_ACTIVITY_STATUS", StringType(), True),
StructField("Source_Sent_Date", StringType(), True),
StructField("Source_Sent_Time", StringType(), True)
])

filelist1=filelist[1:-1]
print(filelist1)
for file in filelist1.split(","):
    foldername=file[1:-5]
    print(foldername)
    dfo=foldername.split("_")
    df=spark.read.format('csv').option('delimiter',"|").schema(dfSchema).load(mountPoint+SourcePath+"/"+foldername+".TXT")
    #df.show()
    df1=df.toPandas()
    df1.insert(29,'Source_Sent_Date',dfo[4],True)
    df1.insert(30,'Source_Sent_Time',dfo[5],True)
    df1=df1[1:-1]
    #display(df1)
    df2=spark.createDataFrame(df1,schema=dfSchema1)
    #display(df2)
    df2.write.mode('append').format("parquet").save(tgtmountPoint+DestinationPath+"/"+RunDate+RunTime+"/")
    df_tmp = df.collect()[0][0]
  

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
