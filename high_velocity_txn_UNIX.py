from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import pandas as pd

if __name__ == "__main__":
    spark = SparkSession\
    .builder\
    .appName("HighVelocityTxn").config('spark.sql.codegen.wholeStage', 'false').getOrCreate()
    
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
        .option("path", "./data/frauddetection")\
        .schema(schema)\
        .load()

    withTime = Df1.withColumn('TxnTimestamp', from_unixtime('TxnTime').cast('timestamp'))
    
    @pandas_udf("start timestamp, end timestamp, CardNum long, HighVelocity Boolean", functionType=PandasUDFType.GROUPED_MAP)
    def identify_high_velocity(pdf):
        #print(pdf.columns)
        
        maxtxn = 3
        return pd.DataFrame([[pdf['TxnTimestamp'].iloc[0], pdf['TxnTimestamp'].iloc[-1], pdf['CardNum'].iloc[0], len(pdf.index)>=maxtxn]], \
            columns = ['start', 'end', 'CardNum', 'HighVelocity'])
        
    
    winsize = "10 seconds"
    movingCount = withTime\
        .groupBy(col('CardNum'), window(col('TxnTimestamp'), winsize, "1 second"))\
            .apply(identify_high_velocity)
            #.explain(True)
                #.orderBy("window.start")#.explain(True)

    result = movingCount.join(withTime,[withTime.CardNum == movingCount.CardNum, \
        withTime.TxnTimestamp >= movingCount.start, withTime.TxnTimestamp <= movingCount.end])\
            .where(col('HighVelocity')==True)\
                .select(withTime.CardNum,'TxnID','TxnTimestamp')
    

    startTime = time.time()
    query = result.writeStream.option('numRows', 600).format("console").trigger(once=True).start().awaitTermination() 
    endTime = time.time()
    print(endTime - startTime)