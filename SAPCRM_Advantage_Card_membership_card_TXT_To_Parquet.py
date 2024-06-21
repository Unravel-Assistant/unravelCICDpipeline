# Databricks notebook source
# MAGIC %md
# MAGIC CONVERSION OF TEXT TO PARQUET

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
dfSchema = StructType([ StructField("record_type", StringType(), True), StructField("CARD_NUMBER", StringType(), True), StructField("CHECK_DIGIT", StringType(), True), StructField("STATUS_CODE", StringType(), True), StructField("CUSTOMER_NUMBER", StringType(), True), StructField("DESP_TO_CUST_DATE", StringType(), True), StructField("CFA_ID", StringType(), True), StructField("ACCOUNT_NUMBER", StringType(), True), StructField("PROD_BATCH_NUMBER", StringType(), True), StructField("GEOGRAPHY_CODE", StringType(), True), StructField("ADVCD_TMNTD_TMSTMP", StringType(), True), StructField("CARD_TYPE_CODE", StringType(), True), StructField("CARD_ACTIVATION_DATE", StringType(), True), StructField("CARD_EXPIRY_DATE", StringType(), True), StructField("CHANGE_USER", StringType(), True), StructField("CREATED_BY_CHANNEL", StringType(), True), StructField("UPDATED_BY_CHANNEL", StringType(), True)])
dfSchema1 = StructType([ StructField("record_type", StringType(), True), StructField("CARD_NUMBER", StringType(), True), StructField("CHECK_DIGIT", StringType(), True), StructField("STATUS_CODE", StringType(), True), StructField("CUSTOMER_NUMBER", StringType(), True), StructField("DESP_TO_CUST_DATE", StringType(), True), StructField("CFA_ID", StringType(), True), StructField("ACCOUNT_NUMBER", StringType(), True), StructField("PROD_BATCH_NUMBER", StringType(), True), StructField("GEOGRAPHY_CODE", StringType(), True), StructField("ADVCD_TMNTD_TMSTMP", StringType(), True), StructField("CARD_TYPE_CODE", StringType(), True), StructField("CARD_ACTIVATION_DATE", StringType(), True), StructField("CARD_EXPIRY_DATE", StringType(), True), StructField("CHANGE_USER", StringType(), True), StructField("CREATED_BY_CHANNEL", StringType(), True), StructField("UPDATED_BY_CHANNEL", StringType(), True),StructField("file_no", StringType(), True)])
filelist1=filelist[1:-1]
print(filelist1)
for file in filelist1.split(","):
    foldername=file[1:-5]
    print(foldername)
    dfo=foldername.split("_")
    #print(dfo)
    df=spark.read.format('csv').option('delimiter',"|").schema(dfSchema).load(mountPoint+SourcePath+"/"+foldername+".TXT")
    #display(df)
    df1=df.toPandas()
    df1.insert(17,'file_no',dfo[3],True)
    df1=df1[1:-1]
    #display(df1)
    df2=spark.createDataFrame(df1,schema=dfSchema1)
    display(df2)
    df2.write.mode('append').format("parquet").save(tgtmountPoint+DestinationPath+"/"+RunDate+RunTime+"/")
    df_pk = df.collect()[0][0]
  

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
