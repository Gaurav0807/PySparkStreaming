from pyspark.sql import SparkSession


if __name__=="__main__":
    spark = SparkSession.builder.appName("spark_streaming").config("spark.sql.shuffle.partitions",3)\
        .config("spark.streaming.stopGracefullyOnShutdown",True).getOrCreate()

    df_ = spark.readStream.format("socket").option("host","localhost").option("port","9093").load()

    df1 = df_.selectExpr("explode(split(value,' ')) as word")
    df2 = df1.groupBy("word").count()

    result = df2.writeStream.format("console").outputMode("complete").option("checkpointLocation","checkppoint-location1")\
        .trigger(processingTime="30 seconds").start()
    result.awaitTermination()


