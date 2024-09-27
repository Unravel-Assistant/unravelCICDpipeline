# Databricks notebook source
# MAGIC %md
# MAGIC CONVERSION OF TEXT TO PARQUET

# COMMAND ----------

# MAGIC %md
# MAGIC select * from curate_standard.SAPCOE_SAPCRM_BUK_SAPCRM_Advantage_Card_customers -- order by rundatetime desc
# MAGIC --ALTER TABLE curate_standard.SAPCOE_SAPCRM_BUK_SAPCRM_Advantage_Card_customers ADD columns (Source_Sent_Date string, Source_Sent_Time string);
# MAGIC --describe curate_standard.SAPCOE_SAPCRM_BUK_SAPCRM_Advantage_Card_customers 

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
dfSchema = StructType([ StructField("RECORD_TYPE", StringType(), True), StructField("BUSINESS_PARTNER_ID", StringType(), True), StructField("TITLE", StringType(), True),StructField("BUSINESS_PARTNER_ROLE", StringType(), True),StructField("USERID", StringType(), True),StructField("INIT", StringType(), True),StructField("FORENAME", StringType(), True),StructField("SURNAME", StringType(), True),StructField("GENDER", StringType(), True),StructField("DATE_OF_BIRTH", StringType(), True),StructField("DATE_OF_DEATH", StringType(), True),StructField("NO_IN_HOUSEHOLD", StringType(), True),StructField("DONOR_FLAG", StringType(), True),StructField("APPL_SOURCE_CODE", StringType(), True),StructField("TRAFFIC_LIGHT", StringType(), True),StructField("DO_NOT_MAIL", StringType(), True),StructField("GONE_AWAY_DATE", StringType(), True),StructField("HOUSE_NUMBER", StringType(), True),StructField("STREET", StringType(), True),StructField("UNIT_NAME", StringType(), True),StructField("BUILDING_NAME", StringType(), True),StructField("TOWN", StringType(), True),StructField("DISTRICT", StringType(), True),StructField("COUNTY", StringType(), True),StructField("POST_CODE", StringType(), True),StructField("ADDRESS_TYPE", StringType(), True),StructField("COUNTRY_CODE", StringType(), True),StructField("PHONE_NUMBER", StringType(), True),StructField("MOBILE_NUMBER", StringType(), True),StructField("FAX_NUMBER", StringType(), True),StructField("EMAIL_ADDRESS", StringType(), True),StructField("CORRECTIVE_EYEWEAR", StringType(), True),StructField("CONTACT_PREFERENCE", StringType(), True),StructField("CREATE_USER", StringType(), True),StructField("CHANGE_USER", StringType(), True),StructField("CREATED_BY_CHANNEL", StringType(), True),StructField("UPDATED_BY_CHANNEL", StringType(), True)])

dfSchema1 = StructType([ StructField("RECORD_TYPE", StringType(), True),
StructField("BUSINESS_PARTNER_ID", StringType(), True),
StructField("TITLE", StringType(), True),
StructField("BUSINESS_PARTNER_ROLE", StringType(), True),
StructField("USERID", StringType(), True),
StructField("INIT", StringType(), True),
StructField("FORENAME", StringType(), True),
StructField("SURNAME", StringType(), True),
StructField("GENDER", StringType(), True),
StructField("DATE_OF_BIRTH", StringType(), True),
StructField("DATE_OF_DEATH", StringType(), True),
StructField("NO_IN_HOUSEHOLD", StringType(), True),
StructField("DONOR_FLAG", StringType(), True),
StructField("APPL_SOURCE_CODE", StringType(), True),
StructField("TRAFFIC_LIGHT", StringType(), True),
StructField("DO_NOT_MAIL", StringType(), True),
StructField("GONE_AWAY_DATE", StringType(), True),
StructField("HOUSE_NUMBER", StringType(), True),
StructField("STREET", StringType(), True),
StructField("UNIT_NAME", StringType(), True),
StructField("BUILDING_NAME", StringType(), True),
StructField("TOWN", StringType(), True),
StructField("DISTRICT", StringType(), True),
StructField("COUNTY", StringType(), True),
StructField("POST_CODE", StringType(), True),
StructField("ADDRESS_TYPE", StringType(), True),
StructField("COUNTRY_CODE", StringType(), True),
StructField("PHONE_NUMBER", StringType(), True),
StructField("MOBILE_NUMBER", StringType(), True),
StructField("FAX_NUMBER", StringType(), True),
StructField("EMAIL_ADDRESS", StringType(), True),
StructField("CORRECTIVE_EYEWEAR", StringType(), True),
StructField("CONTACT_PREFERENCE", StringType(), True),
StructField("CREATE_USER", StringType(), True),
StructField("CHANGE_USER", StringType(), True),
StructField("CREATED_BY_CHANNEL", StringType(), True),
StructField("UPDATED_BY_CHANNEL", StringType(), True),
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
    df1.insert(37,'Source_Sent_Date',dfo[4],True)
    df1.insert(38,'Source_Sent_Time',dfo[5],True)
    df1=df1[1:-1]
    #display(df1)
    df2=spark.createDataFrame(df1,schema=dfSchema1)
    #display(df2)
    df2.write.mode('append').format("parquet").save(tgtmountPoint+DestinationPath+"/"+RunDate+RunTime+"/")
    deet_df = df.collect()[0][0]
  

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
