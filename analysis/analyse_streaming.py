from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import re
import json
#import analyse
import config
#import cass 
sc = None
words = None
businessid_food_count_list = []

def main():
    global sc, words
    conf = SparkConf().setMaster('local[2]').setAppName(config.spark['appname']).set("spark.driver.maxResultSize", "0").set("spark.executor.heartbeatInterval","600")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    words = load_wordlist(config.foodlist)
    reviews = stream(ssc, 10)
    #for r in reviews:
        #if len(r) > 0:
            #rd = sc.parallelize(r)
            #process(rd)

def process(rd):
    #print rd.collect()
    empty = rd.isEmpty()
    print empty
    if empty:
        print 'INFO: Empty rdd'
        #return
    else:
        #print 'Not empty'
        rd = rd.map(parse_json)
        #rd.cache()
        #print rd.collect()
        print 'Final result is '
        businessid_food_count_list.append(rd.flatMap(extract_food_items).map(lambda bid_fooditem: (bid_fooditem,1)).reduceByKey(lambda a,b : a + b).map(create_tuple).collect())
        print 'Final result is '
        print businessid_food_count_list

def stream(ssc, duration):
    reviews = []
    kafkaStream = KafkaUtils.createDirectStream(ssc,['google_places'],kafkaParams = {"metadata.broker.list": '152.46.16.173:9092', 'auto.offset.reset': 'smallest'})
    objstream = kafkaStream.map(lambda x: x[1])
    #objstream.foreachRDD(lambda rdd: reviews.append(rdd.collect()))
    objstream.foreachRDD(process)
    ssc.start()
	#ssc.awaitTermination()
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True) 	
    return reviews

def extract_food_items(review):
    food_items = []
    for word in words:
        if word in review['text']:
            business_id = review['business_id']
            food_items.append(business_id + " " + word)
    return food_items

def load_wordlist(filename):
    text = sc.textFile(filename,4)
	#words = text.flatMap(lambda word: word.split("\n"))
    words = text.flatMap(lambda word: word.split("\n")).map(lambda word: word.split(":")[0])
    return words.collect()


def load_reviews(filename):
    text = sc.textFile(filename,4)
    return text


def parse_json(review):
    #print review
    print 'Parsing json'
    #print review
    review = json.loads(review)
    return {'business_id': review['business_id'], 'text': review['text']}


# returns a tuple (business id, food item, count)
def create_tuple(data):
    arr = data[0].split(" ");
    element = {'business_id': arr[0], 'food': arr[1], 'count': data[1]}
	#cass.insert_food_details(element,"Yelp")
    return element

def filterSpecChars(inp):
		#Following approach is inspired from a StackOverflow post
    return re.sub('[^A-Za-z0-9\s]', '', inp).lower()

if __name__=="__main__":
    main()

