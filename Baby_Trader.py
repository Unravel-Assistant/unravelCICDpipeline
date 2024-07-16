# Databricks notebook source
import pandas as pd
import os
import glob
import datetime
from datetime import date, timedelta
from delta.tables import *
from pyspark.sql.functions import max,lit, col

# COMMAND ----------

print(df.collect())

# MAGIC %run "../../Common/Upsert_to_Synapse"

# COMMAND ----------

# MAGIC %run "../../Common/read_from_synapse"

# COMMAND ----------

currentyearmonth=spark.sql("select concat(Year,month) as currentyearmonth from (select cast(year(current_date()) as string) as Year, case when length(month(current_date()))='1' then Concat(0,month(current_date())) else month(current_date()) end as month)l ").first()["currentyearmonth"]

# COMMAND ----------

import datetime
from dateutil.relativedelta import relativedelta
d = datetime.datetime.strptime(currentyearmonth, '%Y%m').date()
last2YearMonth =(d - relativedelta(years=3)).strftime('%Y%m')
LatestTwoMonth =(d - relativedelta(months=2)).strftime('%Y%m')
print(last2YearMonth)
print(LatestTwoMonth)

# COMMAND ----------

ProductDF = synapse_sql("select ProductSourceKey,ProductName,LadderId,Brand,SubLadderId,ProductHierarchy5Name,ProductRecordSourceKey,SubBrand ,SCDActiveFlag,scdstartdate,scdenddate,ProductSKey from con_dwh.DimProduct ")
ProductDF.createOrReplaceTempView('DimProduct')

salesplanDF = synapse_sql("select SalesPlanYearB,SalesPlanPeriodB , SalesPlanWeekEnding,TradingYear,TradingWeek,Date from con_dwh.DimSalesPlan ")
salesplanDF.createOrReplaceTempView('DimSalesPlan')

FTLDF = synapse_sql("select SiteSKey,LoyaltyAccountSKey,BasketSKey,TransactionDate,Units,TISP,ProductSKey,CardFlag,YearMonth from con_dwh.FactTransactionLine where YearMonth>'{0}'".format(LatestTwoMonth))
FTLDF.createOrReplaceTempView('FactTransactionLine')

DSDF = synapse_sql("select  DS.SiteSourceKey,DS.SiteSKey,DS.SCDStartDate,DS.SCDEndDate ,DS.CountryCode  from con_dwh.DimSite ds")
DSDF.createOrReplaceTempView('DimSite')


DBDF = synapse_sql("select distinct BasketSKey,BasketSourceKey,scdstartdate,scdenddate from con_dwh.DimBasket where  BasketSKey in (select distinct BasketSKey from con_dwh.FactTransactionLine where YearMonth>'"+ str(format(LatestTwoMonth)) +''"')")
DBDF.createOrReplaceTempView('DimBasket')

DLADF = synapse_sql("select LoyaltyAccountSourceKey,LoyaltyAccountSKey,scdstartdate,scdenddate from con_dwh.DimLoyaltyAccount")
DLADF.createOrReplaceTempView('DimLoyaltyAccount')



# COMMAND ----------

TempPart_1=spark.sql(" SELECT ProductSourceKey,ProductName,CASE WHEN ((UPPER(LadderId) IN ('BOOTS.COM EXTENDED', 'DRINKS') OR UPPER(Brand) = 'PAEDIASURE') AND ( LOWER(ProductName) LIKE '%0g%' OR LOWER(ProductName) LIKE '%1g%' OR LOWER(ProductName) LIKE '%2g%' OR LOWER(ProductName) LIKE '%3g%' OR LOWER(ProductName) LIKE '%4g%' OR LOWER(ProductName) LIKE '%5g%' OR LOWER(ProductName) LIKE '%6g%' OR LOWER(ProductName) LIKE '%7g%' OR LOWER(ProductName) LIKE '%8g%' OR LOWER(ProductName) LIKE '%9g%' )) THEN 'MILK POWDER' WHEN (UPPER(Brand) = 'PAEDIASURE' AND ( LOWER(ProductName) LIKE '%0ml%' OR LOWER(ProductName) LIKE '%1ml%' OR LOWER(ProductName) LIKE '%2ml%' OR LOWER(ProductName) LIKE '%3ml%' OR LOWER(ProductName) LIKE '%4ml%' OR LOWER(ProductName) LIKE '%5ml%' OR LOWER(ProductName) LIKE '%6ml%' OR LOWER(ProductName) LIKE '%7ml%' OR LOWER(ProductName) LIKE '%8ml%' OR LOWER(ProductName) LIKE '%9ml%' )) OR ProductSourceKey = '6795706' THEN 'MILK READY TO FEED' WHEN (UPPER(LadderId) IN ('BOOTS.COM EXTENDED', 'DRINKS') AND ( LOWER(ProductName) not LIKE '%0g%' AND LOWER(ProductName) not LIKE '%1g%' AND LOWER(ProductName) not LIKE '%2g%' AND LOWER(ProductName) not LIKE '%3g%' AND LOWER(ProductName) not LIKE '%4g%' AND LOWER(ProductName) not LIKE '%5g%' AND LOWER(ProductName) not LIKE '%6g%' AND LOWER(ProductName) not LIKE '%7g%' AND LOWER(ProductName) not LIKE '%8g%' AND LOWER(ProductName) not LIKE '%9g%' ))  OR ProductSourceKey IN ('2055260', '2059754', '4816617', '4816625')  OR UPPER(LadderId) IN ('BABY FOOD OTHER', 'BUDGET', 'BUNDLE') THEN 'OTHER' ELSE LadderId END AS LadderId, CASE WHEN UPPER(SubLadderId) = 'BOOTS.COM EXTENDED' AND (LOWER(ProductName) LIKE '%growing up%' OR LOWER(ProductName) LIKE '%toddler%') THEN 'TODDLER' WHEN UPPER(SubLadderId) = 'BOOTS.COM EXTENDED' AND (LOWER(ProductName) LIKE '%follow on%' OR LOWER(ProductName) LIKE '% fom %') THEN 'FOLLOW ON' WHEN UPPER(SubLadderId) = 'BOOTS.COM EXTENDED' AND LOWER(ProductName) LIKE '%first infant%' THEN 'FIRST INFANT' WHEN UPPER(SubLadderId) = 'BOOTS.COM EXTENDED' AND LOWER(ProductName) LIKE '%hungry%' THEN 'HUNGRY' WHEN UPPER(SubLadderId) IN ('BOOTS.COM EXTENDED', 'BABY FOOD OTHER', 'BUNDLE', 'BUDGET') THEN 'OTHER' WHEN SubLadderId IS NULL THEN 'OTHER' ELSE SubLadderId END AS SubLadderId, Brand, CASE WHEN LOWER(ProductName) LIKE '%profutura%' THEN 'PROFUTURA' ELSE SubBrand END AS SubBrand FROM DimProduct WHERE (ProductHierarchy5Name='Baby milks' OR (ProductHierarchy5Name='Baby Food' AND UPPER(Brand) = 'PAEDIASURE')) AND ProductSourceKey != '7960131' AND UPPER(LadderId) NOT IN ('BUDGET','UNKNOWN') AND LadderId IS NOT NULL AND ProductSourceKey not IN ('7947968', '7948158', '7947941') AND SCDActiveFlag = 1 AND ProductRecordSourceKey = 'BTCSAPMDM' ")
TempPart_1.createOrReplaceTempView('TempPart_1')

# COMMAND ----------

TempPart_2=spark.sql("SELECT A.BasketSourceKey, A.LoyaltyAccountSourceKey, C.SalesPlanYearB*100+ C.SalesPlanPeriodB AS PERIOD, C.SalesPlanWeekEnding, C.TradingYear, C.TradingWeek, A.TransactionDate, B.ProductSourceKey,A.LadderId,A.CardFlag,A.SiteSourceKey,A.CountryCode,A.YearMonth, SUM(A.Units) AS Units, SUM(A.TISP) AS TISP FROM (SELECT nvl(DB.BasketSourceKey,-1) as BasketSourceKey,DLA.LoyaltyAccountSourceKey as LoyaltyAccountSourceKey, FTL.TransactionDate,FTL.YearMonth,nvl(FTL.CardFlag,'N') as CardFlag,nvl(DS.SiteSourceKey,0) as SiteSourceKey,nvl(DS.CountryCode,'-1') as CountryCode,nvl(DP.LadderId,'UNKNOWN') as LadderId, nvl(DP.ProductSourceKey,-1) as ProductSourceKey, nvl(FTL.Units,0.0000) AS Units, nvl(FTL.TISP,0.0000) AS TISP FROM FactTransactionLine FTL LEFT JOIN DimSite DS ON FTL.SiteSKey = DS.SiteSKey LEFT JOIN DimBasket DB ON FTL.BasketSKey = DB.BasketSKey LEFT outer JOIN DimLoyaltyAccount DLA ON FTL.LoyaltyAccountSKey = DLA.LoyaltyAccountSKey LEFT JOIN DimProduct DP ON FTL.ProductSKey = DP.ProductSKey WHERE FTL.TransactionDate BETWEEN cast(date_add(current_date(),-1097) as Date) AND CAST(date_add(current_date(),-6) as Date) AND FTL.TransactionDate BETWEEN DS.SCDStartDate AND DS.SCDEndDate AND FTL.TransactionDate BETWEEN DP.SCDStartDate AND DP.SCDEndDate  AND cast(DS.SiteSourceKey as int) NOT BETWEEN 1700 AND 1799 AND cast(DS.SiteSourceKey as int) NOT BETWEEN 2700 AND 2799 AND cast(DS.SiteSourceKey as int) NOT BETWEEN 3000 AND 3999 AND cast(DS.SiteSourceKey as int) NOT BETWEEN 4101 AND 4199  AND cast(DS.SiteSourceKey as int) NOT BETWEEN 4911 AND 4919 AND cast(DS.SiteSourceKey as int) NOT BETWEEN 4920 AND 4931 ) A INNER JOIN TempPart_1 B ON A.ProductSourceKey = B.ProductSourceKey LEFT JOIN DimSalesPlan C ON A.TransactionDate = C.Date  GROUP BY A.BasketSourceKey, A.LoyaltyAccountSourceKey, C.SalesPlanYearB*100+ C.SalesPlanPeriodB, C.SalesPlanWeekEnding, C.TradingYear, C.TradingWeek, A.TransactionDate, B.ProductSourceKey,A.CardFlag,A.SiteSourceKey,A.CountryCode,A.YearMonth,A.LadderId HAVING SUM(A.Units) >0.0000 AND SUM(A.TISP) >= 0.0000") 
#TempPart_2.createOrReplaceTempView('TempPart_2')

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, '/mnt/self-serve/Temp_Tables/BabyTraders/FactTransactionLine_delta/')
deltaTable.delete(col('YearMonth') >LatestTwoMonth)
TempPart_2.write.option('header','true').partitionBy('YearMonth').mode('append').save('/mnt/self-serve/Temp_Tables/BabyTraders/FactTransactionLine_delta/')

# COMMAND ----------

fact = spark.read.option('header', True).load('/mnt/self-serve/Temp_Tables/BabyTraders/FactTransactionLine_delta/').where(col('YearMonth') >= last2YearMonth)
fact.createOrReplaceTempView('TempPart_2')

# COMMAND ----------

TempTemp1=spark.sql("select LoyaltyAccountSourceKey,period,sum(Units) as Units from TempPart_2 where LadderId='MILK POWDER' AND ProductSourceKey NOT IN ('3365484', '3365824', '5658454', '6587135') group by LoyaltyAccountSourceKey,period having sum(Units)>=20") 
TempTemp1.createOrReplaceTempView('TempTemp1')

# COMMAND ----------

TempTwenty_plus=spark.sql("select distinct LoyaltyAccountSourceKey from  TempTemp1")
TempTwenty_plus.createOrReplaceTempView('TempTwenty_plus')

# COMMAND ----------

TempTxns=spark.sql("select distinct LoyaltyAccountSourceKey, TransactionDate,BasketSourceKey from TempPart_2 where LadderId='MILK POWDER' AND ProductSourceKey NOT IN ('3365484', '3365824', '5658454', '6587135') ")
TempTxns.createOrReplaceTempView('TempTxns')


# COMMAND ----------

TempSummary =spark.sql("select LoyaltyAccountSourceKey, TransactionDate, count(BasketSourceKey) txns from TempTxns group by LoyaltyAccountSourceKey, TransactionDate having count(BasketSourceKey) >=4")
TempSummary.createOrReplaceTempView('TempSummary') 


# COMMAND ----------

Tempfour_plus =spark.sql ("select distinct LoyaltyAccountSourceKey from TempSummary ")
Tempfour_plus.createOrReplaceTempView('Tempfour_plus') 

# COMMAND ----------

TempPart_2_result =spark.sql ("select case when a.LoyaltyAccountSourceKey is null then b.LoyaltyAccountSourceKey else a.LoyaltyAccountSourceKey end as LoyaltyAccountSourceKey from TempTwenty_plus a full outer join Tempfour_plus b on a.LoyaltyAccountSourceKey=b.LoyaltyAccountSourceKey group by case when a.LoyaltyAccountSourceKey is null then b.LoyaltyAccountSourceKey else a.LoyaltyAccountSourceKey end") 
TempPart_2_result.createOrReplaceTempView('TempPart_2_result')  

# COMMAND ----------

TempPart_3=spark.sql (" SELECT A.BasketSourceKey, A.LoyaltyAccountSourceKey, A.CardFlag, A.TradingYear, A.TradingWeek, A.ProductSourceKey,A.CountryCode,case when A.SiteSourceKey = '4910' then 'Online' else 'Stores' end as channel, SUM(A.TISP) AS TISP, SUM(A.Units) AS Units FROM TempPart_2 A where A.TransactionDate BETWEEN  cast(date_add(current_date(),-1097) as Date) AND CAST(current_date() as Date)   GROUP BY A.BasketSourceKey, A.LoyaltyAccountSourceKey, A.CardFlag, A.TradingYear, A.TradingWeek, case when A.SiteSourceKey = '4910' then 'Online' else 'Stores' end, A.ProductSourceKey ,A.CountryCode HAVING SUM(A.Units) > 0 AND SUM(A.TISP) >= 0")
TempPart_3.createOrReplaceTempView('TempPart_3') 
 

# COMMAND ----------

TempPart_3_result =spark.sql (" select a.TradingYear, a.TradingWeek, c.LadderId, c.SubLadderId, c.Brand, c.SubBrand, a.channel, case when a.CountryCode in ('GB','GG','IM','JE','UNKNOWN') then 'UK' else a.CountryCode END as CountryCode, case when b.LoyaltyAccountSourceKey=-1 or b.LoyaltyAccountSourceKey is null then 'Domestic' else 'CBT' end as Status, a.CardFlag, case when a.LoyaltyAccountSourceKey =-1 or a.LoyaltyAccountSourceKey is null then 1 else a.LoyaltyAccountSourceKey end as LoyaltyAccountSourceKey, a.BasketSourceKey, a.TISP, a.Units from TempPart_3 a LEFT outer JOIN   TempPart_2_result b ON  a.LoyaltyAccountSourceKey = b.LoyaltyAccountSourceKey inner join TempPart_1 c on  a.ProductSourceKey=c.ProductSourceKey") 
TempPart_3_result.createOrReplaceTempView('TempPart_3_result') 

# COMMAND ----------

TempPart_4_result =spark.sql ("SELECT distinct  nvl(TradingYear,0) AS TRADING_YEAR_P_KEY,   nvl(TradingWeek,0) AS TRADING_WEEK_P_KEY,   nvl(LadderId,'UNKNOWN') AS LADDER_P_KEY,   nvl(SubLadderId,'UNKNOWN') AS SUB_LADDER_P_KEY,   nvl(Brand,'UNKNOWN') AS BRAND_P_KEY,   nvl(SubBrand,'UNKNOWN') AS SUB_BRAND_P_KEY,   nvl(channel,'UNKNOWN') AS CHANNEL_P_KEY,   nvl(Status,'UNKNOWN') AS TRANSACTION_STATUS_P_KEY, case when   CardFlag='Y' then 'CARD' else 'NONCARD' end AS CARD_FLAG_P_KEY,   nvl(CountryCode,'-1') AS REPORTING_COUNTRY_CODE_P_KEY,nvl(LoyaltyAccountSourceKey,0) AS LOYALTY_ACCOUNT_SOURCE_KEY,   nvl(BasketSourceKey,'UNKNOWN') AS BASKET_SOURCE_KEY,   nvl(TISP,0.0000) AS TISP,   nvl(Units,0.0000) AS UNITS ,current_timestamp as CREATE_DATETIME,cast(null as timestamp) as UPDATE_DATETIME from TempPart_3_result")
TempPart_4_result.createOrReplaceTempView('TempPart_4_result') 


# COMMAND ----------

dwhStagingTable =None
synapse_output_table = 'con_dwh.RPT_BABY_TRADERS'
key_columns = None
deltaName =None
dwhStagingDistributionColumn =None


# COMMAND ----------

truncate_and_load_synapse(TempPart_4_result,'con_dwh.RPT_BABY_TRADERS')
