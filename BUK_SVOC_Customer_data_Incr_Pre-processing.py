
# Databricks notebook source
# MAGIC %md
# MAGIC select * from curate_standard.svoc_customer_data_incr -- order by rundatetime desc
# MAGIC --ALTER TABLE curate_standard.svoc_customer_data_incr ADD columns (Source_Sent_Date string, Source_Sent_Time string);
# MAGIC --describe curate_standard.svoc_customer_data_incr 

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
print(filelist)
SourceFileCount=len(filelist.split(","))
OutputTableList=[]
dfSchema = StructType([ StructField("ENTITY_ID", StringType(), True),
StructField("PARTY_ID", StringType(), True),
StructField("TITLE", StringType(), True),
StructField("FIRST_NAME", StringType(), True),
StructField("MIDDLE_NAME", StringType(), True),
StructField("LAST_NAME", StringType(), True),
StructField("CHOSEN_FIRST_NAME", StringType(), True),
StructField("BIRTH_LAST_NAME", StringType(), True),
StructField("GENDER", StringType(), True),
StructField("DATE_OF_BIRTH", StringType(), True),
StructField("DATE_OF_DEATH", StringType(), True),
StructField("SOURCE_DECEASED_INFO", StringType(), True),
StructField("NOTIFICATION_DATE", StringType(), True),
StructField("THIRD_PARTY_PROVIDED_FLAG", StringType(), True),
StructField("NO_DATA_SHARE_FLAG", StringType(), True),
StructField("NO_DUTY_OF_CARE_FLAG", StringType(), True),
StructField("SPECIAL_FLAG3", StringType(), True),
StructField("SPECIAL_FLAG4", StringType(), True),
StructField("SPECIAL_FLAG5", StringType(), True),
StructField("SPECIAL_FLAG6", StringType(), True),
StructField("SPECIAL_FLAG7", StringType(), True),
StructField("SPECIAL_FLAG8", StringType(), True),
StructField("SPECIAL_FLAG9", StringType(), True),
StructField("SPECIAL_FLAG10", StringType(), True),
StructField("ENTITY_CREATE_TIME", StringType(), True),
StructField("ENTITY_LAST_UPDATE_TIME", StringType(), True)
])

dfSchema1 = StructType([ StructField("ENTITY_ID", StringType(), True),
StructField("PARTY_ID", StringType(), True),
StructField("TITLE", StringType(), True),
StructField("FIRST_NAME", StringType(), True),
StructField("MIDDLE_NAME", StringType(), True),
StructField("LAST_NAME", StringType(), True),
StructField("CHOSEN_FIRST_NAME", StringType(), True),
StructField("BIRTH_LAST_NAME", StringType(), True),
StructField("GENDER", StringType(), True),
StructField("DATE_OF_BIRTH", StringType(), True),
StructField("DATE_OF_DEATH", StringType(), True),
StructField("SOURCE_DECEASED_INFO", StringType(), True),
StructField("NOTIFICATION_DATE", StringType(), True),
StructField("THIRD_PARTY_PROVIDED_FLAG", StringType(), True),
StructField("NO_DATA_SHARE_FLAG", StringType(), True),
StructField("NO_DUTY_OF_CARE_FLAG", StringType(), True),
StructField("SPECIAL_FLAG3", StringType(), True),
StructField("SPECIAL_FLAG4", StringType(), True),
StructField("SPECIAL_FLAG5", StringType(), True),
StructField("SPECIAL_FLAG6", StringType(), True),
StructField("SPECIAL_FLAG7", StringType(), True),
StructField("SPECIAL_FLAG8", StringType(), True),
StructField("SPECIAL_FLAG9", StringType(), True),
StructField("SPECIAL_FLAG10", StringType(), True),
StructField("ENTITY_CREATE_TIME", StringType(), True),
StructField("ENTITY_LAST_UPDATE_TIME", StringType(), True),
StructField("Source_Sent_Date", StringType(), True),
StructField("Source_Sent_Time", StringType(), True),
])
filelist1=filelist[1:-1]
print(filelist1)
for file in filelist1.split(","):
    foldername=file[1:-5]
    print(foldername)
    dfo=foldername.split("_")
   # print(dfo)    dfo=dfo[3]    print(dfo)    print(dfo[4])
    df=spark.read.format('csv').option('delimiter',"|").schema(dfSchema).load(mountPoint+SourcePath+"/"+foldername+".psv")
   # df.show()
    df1=df.toPandas()
    df1.insert(26,'Source_Sent_Date',dfo[3],True)
    df1.insert(27,'Source_Sent_Time',dfo[4],True)
    df1=df1[1:]
    df2=spark.createDataFrame(df1,schema=dfSchema1)
    display(df2)
    df2.write.mode('append').format("parquet").save(tgtmountPoint+DestinationPath+"/"+RunDate+RunTime+"/")
    

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
