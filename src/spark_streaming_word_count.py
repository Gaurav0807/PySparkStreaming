from pyspark.sql import SparkSession


if __name__=="__main__":
    spark = SparkSession.builder.appName("spark_streaming").config("spark.sql.shuffle.partitions",3)\
        .getOrCreate()

    df_ = spark.readStream.format("socket").option("host","localhost").option("port","9092").load()

    #split will always give array
    #Explode on the array to get each words in different row.

    df1 = df_.selectExpr("explode(split(value,' ')) as word")

    df2 = df1.groupBy("word").count()

    ##write to console
    result = df2.writeStream.format("console").outputMode("complete")\
        .option("checkpointLocation","checkppoint-location1")\
        .start()


    result.awaitTermination()


