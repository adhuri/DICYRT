from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import analyse
sc = None

def main():
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    
    kafkaStream = KafkaUtils.createDirectStream(ssc,['google_places'],kafkaParams = {"metadata.broker.list": '152.46.16.173:9092', 'auto.offset.reset': 'smallest'})
    gplaces = kafkaStream.foreachRDD(lambda x: pprint(x))
   	
    ssc.start()             
    ssc.awaitTermination()  	

def pprint(x):
    print x.collect()
    #analysis to be done on this rdd
    #analyse.perform_analysis(x)
    
if __name__=="__main__":
    main()

