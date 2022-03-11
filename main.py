
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__=="__main__":

    sc = SparkContext("local[*]","word_count")
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc,5)

    lines = ssc.socketTextStream("localhost",9098)
    words = lines.flatMap(lambda x:x.split(" "))
    pairs = words.map(lambda x:(x,1))
    word_counts=pairs.reduceByKey(lambda x,y:x+y)
    word_counts.pprint()
    ssc.start()

    ssc.awaitTermination()



