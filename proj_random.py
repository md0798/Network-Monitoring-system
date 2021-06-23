from socket import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import sum, col, stddev_pop, avg, desc, expr, asc
import random

sc = SparkContext("local", "myApp")
spark = SparkSession.builder.appName('project').getOrCreate()

port = 1234
host = 'localhost'
serverSocket = socket(AF_INET, SOCK_STREAM)
serverSocket.bind((host, port))
serverSocket.listen()
print('Ready to receive')
conn, addr = serverSocket.accept()

all_list = []

userSchema = StructType()\
    .add('Time', 'integer').add('Duration', 'integer')\
    .add('SrcDevice', 'string').add('DstDevice', 'string')\
    .add('Protocol', 'integer')\
    .add('SrcPort', 'string').add('DstPort', 'string')\
    .add('SrcPackets', 'integer').add('DstPackets', 'integer')\
    .add('SrcBytes', 'integer').add('DstBytes', 'integer')

percentage = 0.01
top_k = 5
X_times = 1

# Random Sampling (e.g. Reservoir Sampling)
sample_size = 10000

def isValid(check_row):
    for i in range(7, 11):
        if check_row[i] < -2147483648 or check_row[i] > 2147483647:
            print("ABANDON ROW")
            return False
    return True

def parseMsg(msg):
    r = []
    r.append(int(msg.split(',')[0]))
    r.append(int(msg.split(',')[1]))
    r.append(msg.split(',')[2])
    r.append(msg.split(',')[3])
    r.append(int(msg.split(',')[4]))
    r.append(msg.split(',')[5])
    r.append(msg.split(',')[6])
    r.append(int(msg.split(',')[7]))
    r.append(int(msg.split(',')[8]))
    r.append(int(msg.split(',')[9]))
    r.append(int(msg.split(',')[10]))
    return r

counter = 0
while True:
    counter += 1
    message = conn.recv(2048).decode()
    if message.startswith("#"):
        message = message.lstrip("#")
        try:
            row = parseMsg(message)
        except ValueError or IndexError as e:
            print("ValueError or IndexError captured")
            continue

        if isValid(row):
            if len(all_list) < sample_size:
                all_list.append(row)
            else:
                index_to_remove = random.randint(0, counter-1)
                if index_to_remove < sample_size:
                    all_list[index_to_remove] = row

all_df = spark.createDataFrame(all_list, schema=userSchema)
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