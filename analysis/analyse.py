from pyspark import SparkConf, SparkContext
import re
import json
import config

sc = None
words = None


def extract_food_items(review):
    food_items = []
    for word in words:
        if word in review['text']:
            business_id = review['business_id']
            food_items.append(business_id + " " + word)
    return food_items


def load_wordlist(filename):
    text = sc.textFile(filename,4)
    words = text.flatMap(lambda word: word.split("\n"))
    return words.collect()


def load_reviews(filename):
    text = sc.textFile(filename,4)
    return text


def parse_json(review):
    review = json.loads(review)
    return {'business_id': review['business_id'], 'text': review['text']}


# returns a tuple (business id, food item, count)
def create_tuple(data):
    arr = data[0].split(" ");
    return {'business_id': arr[0], 'food': arr[1], 'count': data[1]}


def main():
    global sc, words
    conf = SparkConf().setMaster(config.spark['server']).setAppName(config.spark['appname']).set("spark.driver.maxResultSize", "0").set("spark.executor.heartbeatInterval","600")
    #conf = SparkConf().setMaster('local[2]').setAppName(config.spark['appname']).set("spark.driver.maxResultSize", "0").set("spark.executor.heartbeatInterval","600")
    sc = SparkContext(conf=conf)
    words = load_wordlist(config.foodlist)
    reviews = load_reviews(config.reviewlist)
    reviews = reviews.map(parse_json)
    reviews.cache()
    businessid_food_count = reviews.flatMap(extract_food_items).map(lambda bid_fooditem: (bid_fooditem,1)).reduceByKey(lambda a,b : a + b).map(create_tuple)
    print businessid_food_count.collect()[0:10]
    

def filterSpecChars(inp):
        #Following approach is inspired from a StackOverflow post
	return re.sub('[^A-Za-z0-9\s]', '', inp).lower()

main()
