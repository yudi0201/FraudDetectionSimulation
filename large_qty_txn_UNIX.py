from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.conf import SparkConf
import time
import pandas as pd

if __name__ == "__main__":
    spark = SparkSession\
    .builder\
    .appName("LargeQtyTxn").config('spark.sql.codegen.wholeStage', 'false').getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    #spark.sparkContext.setLogLevel("TRACE")

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    
    schema = StructType([StructField("TxnTime", DoubleType(), True),\
        StructField("TxnID", LongType(), True),\
        StructField("ItemNo", LongType(), True),\
        StructField("Qty", LongType(), True),\
        StructField("CardNum",LongType(), True)])

    Df1 = spark\
        .readStream\
        .format("csv")\
        .option("maxFilesPerTrigger", 2)\
        .option("header", True)\
        .option("path", "./data/spark_stream_dir")\
        .schema(schema)\
        .load()
    
    withTime = Df1.withColumn('TxnTimestamp', from_unixtime('TxnTime').cast('timestamp'))

    # calculates average and stdev of quantities for each item in windows of 10 seconds.
    # start and end are NOT the startTime and endTime of each window, but rather the time of the earliest and latest txn in each window.
    # There doesn't seem to be a way to return the startTime and endTime of each window (which would be preferable in this case) in a GROUPED_MAP UDF like this.
    @pandas_udf("start timestamp, end timestamp, Item long, avgQty double, stdevQty double", functionType=PandasUDFType.GROUPED_MAP)
    def calculate_stats(pdf):
        #print(pdf.columns)
        return pd.DataFrame([[pdf['TxnTimestamp'].iloc[0], pdf['TxnTimestamp'].iloc[-1], pdf['ItemNo'].iloc[0], pdf['Qty'].mean(), pdf['Qty'].std()]], \
            columns = ['start', 'end', 'Item', 'avgQty', "stdevQty"])
    
    
    winsize = "50 seconds"
    QtyStat = withTime\
        .groupBy(col('ItemNo'), window(col('TxnTimestamp'), winsize, "1 second"))\
            .apply(calculate_stats)
            #.explain(True)
                #.orderBy("window.start")#.explain(True)

    # JOIN the average and stdev results with the original stream. The join criteria will map each txn to multiple
    # windows (because start and end are the earliest and latest txn times in each window, not the actual window startTime and endTime), 
    # therefore an additional step is needed below to identify the one correct window to map each txn to.
    temp_result1 = QtyStat.join(withTime,[withTime.ItemNo == QtyStat.Item, \
        withTime.TxnTimestamp > QtyStat.end, withTime.TxnTimestamp <= QtyStat.start + expr('INTERVAL 50 SECONDS')])\
            .select('start', 'end', 'TxnID', "Qty", "avgQty", "stdevQty")

    # match each txn with the correct window by taking the maximum of the endTimes and minimum of the startTimes
    @pandas_udf("start timestamp, end timestamp, TxnID long", functionType=PandasUDFType.GROUPED_MAP)
    def identify_correct_frame(pdf):
        #print(pdf.columns)
        return pd.DataFrame([[pdf['start'].min(), pdf['end'].max(), pdf['TxnID'].iloc[0]]], \
            columns = ['start', 'end', 'TxnID'])   
    
    temp_result2 = temp_result1.groupBy(col('TxnID')).apply(identify_correct_frame)

    # Finally, join the txns mapped with their correct windows (i.e. temp_result2) with the avg and stdev data in temp_result1
    # and apply the criteria (Qty > avgQty + 3 * stdevQty) to identify fraudulent txns. 
    result = temp_result2.join(temp_result1, [temp_result2.start == temp_result1.start, \
        temp_result2.end == temp_result1.end, temp_result2.TxnID == temp_result1.TxnID])\
            .where(col('Qty') > col('avgQty') + 3*col('stdevQty'))\
                .select(temp_result2.TxnID, "Qty", "avgQty", "stdevQty")

    startTime = time.time()
    query = result.writeStream.option('numRows', 200).format("console").trigger(once=True).start().awaitTermination() 
    endTime = time.time()
    print(endTime - startTime)