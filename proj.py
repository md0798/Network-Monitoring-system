from socket import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import sum, col, stddev_pop, avg, desc, expr, asc
import traceback
import logging
import streamlit as st
import pandas as pd

@st.cache
def bind_socket():
    port = 1234
    host = 'localhost'
    serverSocket = socket(AF_INET, SOCK_STREAM)
    serverSocket.bind((host, port))
    serverSocket.listen()
    print('Ready to receive')
    conn, addr = serverSocket.accept()
    return conn

try:
    sc = SparkContext.getOrCreate();
    spark = SparkSession.builder.appName('project').getOrCreate()
	
    conn = bind_socket()
    st.title("Network Monitoring System")

    window = []
    all_list = []

    userSchema = StructType()\
        .add('Time', 'integer').add('Duration', 'integer')\
        .add('SrcDevice', 'string').add('DstDevice', 'string')\
        .add('Protocol', 'integer')\
        .add('SrcPort', 'string').add('DstPort', 'string')\
        .add('SrcPackets', 'integer').add('DstPackets', 'integer')\
        .add('SrcBytes', 'integer').add('DstBytes', 'integer')

    window_size = st.slider("Window size", min_value=100,max_value=500, step = 200) # T
    # 1. List devices that are consuming more than H percent of the total external bandwidth over the last T time units.
    percentage = st.slider('Percentage',min_value=0.0,max_value=1.0,step=0.1,value=0.1)  # H
    # 2. List the top-k most resource intensive devices over the last T time units.
    top_k = st.slider('Top K values',min_value=1,max_value=5,step=1, value=3)
    # 3. List all devices that are consuming more than X times the standard deviation of the average traffic consumption
    # of all devices over the last T time units.
    X_times = st.slider('X times standard deviation',min_value=0, max_value= 10,step=1,value=2)

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
    q1 = st.empty()
    q2 = st.empty()
    q3 = st.empty()
    q4 = st.empty()
    q5 = st.empty()
    q6 = st.empty()
    q7 = st.empty()
    q8 = st.empty()
    q9 = st.empty()
    graph1 = st.empty()
    graph2 = st.empty()
    graph3 = st.empty()
    graph4 = st.empty()
    graph5 = st.empty()
    graph6 = st.empty()
    while True:
        while len(window) < window_size:
            message = conn.recv(2048).decode()
            if message.startswith("#"):
                message = message.lstrip("#")
                try:
                    row = parseMsg(message)
                except ValueError or IndexError as e:
                    print("ValueError or IndexError captured")
                    continue

                if isValid(row):
                    window.append(row)
                    all_list.append(row)

        # WINDOWED
        window_df = spark.createDataFrame(window, schema=userSchema)
        window_df = window_df.drop('Time', 'Duration', 'Protocol', 'SrcPort', 'DstPort', 'SrcBytes', 'DstBytes')

        #print(window_df.count())
        result_df1 = window_df.groupBy("SrcDevice").agg(sum("SrcPackets").alias("TotalSrcPkts"))
        result_df2 = window_df.groupBy("SrcDevice").agg(sum("DstPackets").alias("TotalDstPkts"))
        result_df = result_df1.join(result_df2, ["SrcDevice"])
        result_df = result_df.withColumn('TotalPackets', expr("TotalSrcPkts + TotalDstPkts"))
        result_df = result_df.drop("TotalSrcPkts", "TotalDstPkts")
        result_df.show()
        

        sum_value = result_df.groupBy().agg(sum("TotalPackets")).collect()[0][0]
        query1_df = result_df.filter(result_df['TotalPackets'] >= percentage*sum_value)
        query1_df_1 = query1_df.select("SrcDevice","TotalPackets").toPandas()
        query1_df_1 = query1_df_1.rename(columns={'SrcDevice':'index'}).set_index('index')

        query2_df = result_df.sort(desc("TotalPackets")).limit(top_k)
        #query2_df.show()
        query2_df_1 = query2_df.select("SrcDevice","TotalPackets").toPandas()
        query2_df_1 = query2_df_1.rename(columns={'SrcDevice':'index'}).set_index('index')
               
        

        stddev_value = result_df.groupBy().agg(stddev_pop("TotalPackets")).collect()[0][0]
        avg_value = result_df.groupBy().agg(avg("TotalPackets")).collect()[0][0]
        query3_df = result_df.filter(result_df['TotalPackets'] >= (X_times*stddev_value)+avg_value)
        #query3_df.show()
        query3_df_1 = query3_df.select("SrcDevice","TotalPackets").toPandas()
        query3_df_1 = query3_df_1.rename(columns={'SrcDevice':'index'}).set_index('index')
        

        # sliding window
        window = window[10:]


        # OVER TIME
        all_df = spark.createDataFrame(all_list, schema=userSchema)
        all_df = all_df.drop('Time', 'Duration', 'Protocol', 'SrcPort', 'DstPort', 'SrcBytes', 'DstBytes')

        all_result_df1 = all_df.groupBy("SrcDevice").agg(sum("SrcPackets").alias("TotalSrcPkts"))
        all_result_df2 = all_df.groupBy("SrcDevice").agg(sum("DstPackets").alias("TotalDstPkts"))
        all_result_df = all_result_df1.join(all_result_df2, ["SrcDevice"])
        all_result_df = all_result_df.withColumn('TotalPackets', expr("TotalSrcPkts + TotalDstPkts"))
        all_result_df = all_result_df.drop("TotalSrcPkts", "TotalDstPkts")
        #all_result_df.show()

        all_sum_value = all_result_df.groupBy().agg(sum("TotalPackets")).collect()[0][0]
        all_query1_df = all_result_df.filter(all_result_df['TotalPackets'] >= percentage*all_sum_value)
        #all_query1_df.show()
        aquery1_df_1 = all_query1_df.select("SrcDevice","TotalPackets").toPandas()
        aquery1_df_1 = aquery1_df_1.rename(columns={'SrcDevice':'index'}).set_index('index')
        

        all_query2_df = all_result_df.sort(desc("TotalPackets")).limit(top_k)
        all_query2_df.show()
        aquery2_df_1 = all_query2_df.select("SrcDevice","TotalPackets").toPandas()
        aquery2_df_1 = aquery2_df_1.rename(columns={'SrcDevice':'index'}).set_index('index')
        

        all_stddev_value = all_result_df.groupBy().agg(stddev_pop("TotalPackets")).collect()[0][0]
        all_avg_value = all_result_df.groupBy().agg(avg("TotalPackets")).collect()[0][0]
        all_query3_df = all_result_df.filter(all_result_df['TotalPackets'] >= (X_times*all_stddev_value)+all_avg_value)
        #all_query3_df.show()
        aquery3_df_1 = all_query3_df.select("SrcDevice","TotalPackets").toPandas()
        aquery3_df_1 = aquery3_df_1.rename(columns={'SrcDevice':'index'}).set_index('index')
        
        
        
        q1.empty()
        q1 = st.header("Query 1 - Devices using more than H percent")
        q4.empty()
        q4 = st.subheader("Window")
        graph1.empty()
        graph1 = st.bar_chart(query1_df_1)
        
        q5.empty()
        q5 = st.subheader("Over time")
        graph4.empty()
        graph4 = st.bar_chart(aquery1_df_1)
        
        q2.empty()
        q2 = st.header("Query 2 - Top K resource intensive devices")
        q6.empty()
        q6 = st.subheader("Window")
        graph2.empty()
        graph2 = st.bar_chart(query2_df_1) 
        q7.empty()
        q7 = st.subheader("Over time")
        graph5.empty()
        graph5 = st.bar_chart(aquery2_df_1)
        
        q3.empty()
        q3 = st.header("Query 3 - Devices consuming more than X times of std. deviation")
        q8.empty()
        q8 = st.subheader("Window")
        graph3.empty()
        graph3 = st.bar_chart(query3_df_1)
        q9.empty()
        q9 = st.subheader("Over time")
       
        graph6.empty()
        graph6 = st.bar_chart(aquery3_df_1)

except OSError:
    print("OS")
    
