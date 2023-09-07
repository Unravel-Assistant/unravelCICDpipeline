# Databricks notebook source
import pandas as pd
import os
import glob
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "../Common/Upsert_to_Synapse"

# COMMAND ----------

Source_Monthly='/mnt/self-serve/BUKMI/SAPBW/BootUK_Exec_Scorecard_Monthly_Actuals_SAPBW/Default/BI2500_Exec_Scorecard_Monthly*'
MonthlyDF=spark.read.format('csv').option('header',True).load(Source_Monthly)
MonthlyDF.createOrReplaceTempView('Monthly_Exec1')

SourceWeekly='/mnt/self-serve/BUKMI/BUKMIEXCEL/BI2500_Exec_Scorecard/Default/BI2500_Exec_Scorecard*'
WeeklyDF=spark.read.format('csv').option('header',True).load(SourceWeekly)
WeeklyDF.createOrReplaceTempView('Weekly_Exec_Date1')

# COMMAND ----------

monthDF= spark.sql("select to_timestamp(date ,'d/M/yyyy') as `date` from Monthly_Exec1")
monthDF.createOrReplaceTempView('Monthly_Exec')
weekDF= spark.sql("select to_timestamp(date ,'d/M/yyyy') as `date` from Weekly_Exec_Date1")
weekDF.createOrReplaceTempView('Weekly_Exec_Date')

# COMMAND ----------

df4=spark.sql("select distinct d.WeeklyDate as WeeklyDate ,c.MaxmonthlyDate as MaxmonthlyDate,c.PreviousMonthDate as PreviousMonthDate,Current_timestamp as RunDate from (SELECT Date as WeeklyDate,cast(concat(year(date),month(date)) as int) as year_month FROM Weekly_Exec_Date ) d left outer join (select a.MaxmonthlyDate,a.year_month,b.PreviousMonthDate from (select max(date)as MaxmonthlyDate,cast(concat(year(date),month(date)) as int) as year_month from Monthly_Exec group by cast(concat(year(date),month(date)) as int) ) a,(select max(date) as PreviousMonthDate,cast(concat(year(add_months(date,1)),month(add_months(date,1) )) as int) as previous_year_month from Monthly_Exec group by cast(concat(year(add_months(date,1)),month(add_months(date,1))) as int) ) b where a.year_month=b.previous_year_month ) c on  d.year_month=c.year_month")

# COMMAND ----------

df4=df4.withColumn("PreviousMonthDate",when ( col("PreviousMonthDate").isNull(),last(col("MaxmonthlyDate"),ignorenulls=True).over(Window.orderBy("weeklyDate"))).otherwise(col("PreviousMonthDate")))

# COMMAND ----------

df5 = df4.toCollect()[0][0]


dwhStagingTable =None
synapse_output_table = 'SS_BUKMI.Exec_Date'
key_columns = None
deltaName =None
dwhStagingDistributionColumn =None

# COMMAND ----------

truncate_and_load_synapse (df4,synapse_output_table)
