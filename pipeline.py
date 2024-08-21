from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.pandas import DataFrame as PandasOnSparkDF
import time

# Define the parameters
numRows = 10  # Set the number of rows you want to process
numPartitions = 1 # Set the number of partitions

# Enabling Hive support to connect with the Hive metastore
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

df = spark.range(0, numRows, 1, numPartitions).selectExpr("id", "(id % 100) as sym")

df.createOrReplaceGlobalTempView("t1")
df1 = spark.sql("SELECT COUNT(1), sym FROM global_temp.t1 GROUP BY sym")

def process_partition(partition):
    return partition

df1.rdd.foreachPartition(process_partition)

df1.show()

# Convert to Pandas DataFrame
# pandas_df = df1.toPandas()
pandas_df = PandasOnSparkDF(df1)

# Get the number of rows in the Pandas DataFrame
rows_count = pandas_df.shape[0]
print(f"Number of rows: {rows_count}")

# time.sleep(40)
