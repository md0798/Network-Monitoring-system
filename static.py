from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import sum, col, stddev_pop, avg, desc, expr, asc

sc = SparkContext("local", "myApp")
spark = SparkSession.builder.appName('project').getOrCreate()

percentage = 0.1
top_k = 3
X_times = 20

userSchema = StructType()\
        .add('Time', 'integer').add('Duration', 'integer')\
        .add('SrcDevice', 'string').add('DstDevice', 'string')\
        .add('Protocol', 'integer')\
        .add('SrcPort', 'string').add('DstPort', 'string')\
        .add('SrcPackets', 'integer').add('DstPackets', 'integer')\
        .add('SrcBytes', 'integer').add('DstBytes', 'integer')

all_df = spark.read.format("csv").schema(userSchema).load("input.csv").limit(25000000)

all_df = all_df.drop('Time', 'Duration', 'Protocol', 'SrcPort', 'DstPort', 'SrcBytes', 'DstBytes', 'DstDevice')

all_result_df1 = all_df.groupBy("SrcDevice").agg(sum("SrcPackets").alias("TotalSrcPkts"))
all_result_df2 = all_df.groupBy("SrcDevice").agg(sum("DstPackets").alias("TotalDstPkts"))
all_result_df = all_result_df1.join(all_result_df2, ["SrcDevice"])
all_result_df = all_result_df.withColumn('TotalPackets', expr("TotalSrcPkts + TotalDstPkts"))
all_result_df = all_result_df.drop("TotalSrcPkts", "TotalDstPkts")

all_sum_value = all_result_df.groupBy().agg(sum("TotalPackets")).collect()[0][0]
all_query1_df = all_result_df.filter(all_result_df['TotalPackets'] >= percentage*all_sum_value)
all_query1_df.show()

all_query2_df = all_result_df.sort(desc("TotalPackets")).limit(top_k)
all_query2_df.show()

all_stddev_value = all_result_df.groupBy().agg(stddev_pop("TotalPackets")).collect()[0][0]
all_avg_value = all_result_df.groupBy().agg(avg("TotalPackets")).collect()[0][0]
all_query3_df = all_result_df.filter(all_result_df['TotalPackets'] > (X_times*all_stddev_value)+all_avg_value)
all_query3_df.show()