from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import sys
import time

INVALID_USAGE = """
Please enter all command-line parameters!

<numRows> Number of rows to process
<numPartitions> Number of partitions to process
"""

def main(args):
    if len(args) < 2:
        print(INVALID_USAGE)
        sys.exit(-1)

    # Enabling Hive support to connect with the Hive metastore
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    numRows = int(args[0])
    numPartitions = int(args[1])

    df = spark.range(0, numRows, 1, numPartitions).selectExpr("id", "(id % 100) as sym")

    df.createOrReplaceGlobalTempView("t1")
    df1 = spark.sql("SELECT COUNT(1), sym FROM global_temp.t1 GROUP BY sym")

    def process_partition(partition):
        time.sleep(3)
        return partition

    df1.rdd.foreachPartition(process_partition)

    df1.show()
    pandas_df = PandasOnSparkDF(df1)

if __name__ == "__main__":
    main(sys.argv[1:])
