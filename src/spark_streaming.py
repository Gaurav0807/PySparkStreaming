from pyspark.sql import SparkSession


if __name__=="__main__":
    spark =SparkSession.builder.appName("spark_streaming").getOrCreate()


    ##read the stream

    df_ = spark.readStream.format("socket").option("host","localhost").option("port","9092").load()

    #Print the schema
    df_.printSchema()

    ##write to sink
    result = df_.writeStream.format("console").outputMode("append").start()


    result.awaitTermination()


