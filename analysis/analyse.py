from pyspark import SparkConf, SparkContext
import re
sc = None
words = None

def main():
    global sc, words
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    #ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    #ssc.checkpoint("checkpoint")

    words = load_wordlist("foodDict.txt")
    reviews = load_wordlist("reviews.txt")
    sentiment = stream(words, reviews)
    print sentiment.collect()


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    text = sc.textFile(filename)
    words = text.flatMap(lambda word: word.split("\n"))
    return words.collect()

def filterSpecChars(inp):
	#Following approach is inspired from a StackOverflow post
	return re.sub('[^A-Za-z0-9\s]', '', inp).lower()

def checkWord(word):
    if(word in words):
    	return (word, 1)
    else:
    	return ("none", 1)

def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)

def stream(words, reviews):
    

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    reviews = sc.parallelize(reviews)
    
    tweets_filtered = reviews.map(filterSpecChars)

    tweets_words = tweets_filtered.flatMap(lambda word: word.split(" "))

    #import code;code.interact(local=locals())

    sentiment = tweets_words.map(checkWord)

    sentiment = sentiment.reduceByKey(lambda x, y : (int(x) + int(y)))

    sentiment = sentiment.filter(lambda x : x[0] != "none")

    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    return sentiment
    # counts = []
    
    # sentiment.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))

    # sentiment = sentiment.updateStateByKey(updateFunction)

    # sentiment.pprint()

    # return counts


if __name__=="__main__":
    main()
