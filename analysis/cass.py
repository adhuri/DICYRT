from cassandra.cluster import Cluster 
import operator
import datetime,time
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement, dict_factory, named_tuple_factory
import config,json
from kafka import KafkaProducer
from setting_logs import set_log

cluster = Cluster(
	contact_points=['152.46.19.234'],
	)
session = cluster.connect('aniket')

producer = KafkaProducer(bootstrap_servers='152.46.16.173:9092',value_serializer=lambda v: v.encode('utf-8'))

def get_food_details(b_id,top=10):
	global session
	#setLog("INFO","Getting food_details for business_id = " + b_id +", count "+str(top))
	set_log("INFO", "logs", "Getting food_details for business_id = " + b_id +", count "+str(top))
	get_result_prepared = session.prepare("select business_id,food,count from food_details where business_id=? ")
	results = session.execute(get_result_prepared,[b_id])
	return getDict(b_id,results,top)


def getDict(b_id,results,top):
	dic={}
	for result in results:
		if result.food in dic:
			dic[result.food]+= result.count
		else:
			dic[result.food] = result.count
	#print dic
	sorted_x = sorted(dic.items(), key=operator.itemgetter(1),reverse=True)
	dic2={}
	dic2['food_list'] = sorted_x[:top]
	dic2['business_id'] = b_id
	return dic2


def get_foodcounts(food, business_id):
       try:
           set_log("INFO", "logs", "Getting food_details for business_id = " + b_id +", count "+str(top))
           get_foodcounts_prepared = session.prepare("select count from food_details where business_id=? and food=?")
           result = session.execute(get_foodcounts_prepared, (business_id, food))
           count = result[0].count
           set_log("INFO", "debug", "The count for " + business_id + " and " + food + " is " + count)
           return count
       except Exception as e:
           return 0
           #print e
           set_log("INFO", "debug", "Food" + food +"does not appear in the review for " + business_id)


def get_business_details(city): 
        try:
            set_log("INFO", "logs", "Getting business details for city " + str(city))
            get_business_details_prepared = session.prepare("select name, business_id, full_address, latitude, longitude, stars from business_details where city=?")
            session.row_factory = dict_factory
            results = session.execute(get_business_details_prepared, [city])
            session.row_factory = named_tuple_factory
            result_list = [i for i in results]
            set_log("INFO", "debug", "The business details for city " + city + " are " + result_list)
            return result_list
        except Exception as e:
             print e
			 #setLog("ERROR","Failed to get business details for city " + str(city))
             set_log("ERROR", "debug", "Failed to get business details for city " + str(city))
            

def get_top_restaurants(food, city):
        limit = 10
        business_details = get_business_details(city)
        top_restaurants = []
        for business in business_details:
            count = int(get_foodcounts(food, str(business['business_id'])))
            if count > 0:
                business['count'] = count
                #print business
                top_restaurants.append(business)
        top_restaurants = sorted(top_restaurants, key=lambda restaurant: restaurant['count'], reverse=True)
        l = len(top_restaurants)
        if l < limit:
            limit = l
        return top_restaurants[:limit] 
            

def insert_food_details(element,source):
	try:	
		element['source']=source
		#setLog("INFO", " Inserting into food_details :" + str(element))
		set_log("INFO", "logs", "Inserting into food_details :" + str(element))
		session.execute(
		"""
    		INSERT INTO food_details (business_id, food, count, source)
    		VALUES (%(business_id)s, %(food)s, %(count)s, %(source)s)
    		""",element)
	except:
		#setLog("ERROR", " Failed inserting into food_details :" + str(element))
		set_log("ERROR", "debug", " Failed inserting into food_details :" + str(element))


def get_business_id(name,city):
	#setLog("INFO","Getting  business_id for name : " + name +", city :  "+city)
	set_log("INFO", "logs", "Getting  business_id for name : " + name +", city :  "+city)
	get_businessid_prepared = session.prepare("select business_id from business_details where name=? and city=? ")
	results = session.execute(get_businessid_prepared,(name,city))
	#print results[0]
	try:
		if results[0].business_id:
			return results[0].business_id
		else: 
			return None
	except:
		return None


def insert_business_details(business_list):
	try:	
    		with open(business_list) as f:
        		content = f.readlines()
        		#print content
        		a= json.loads(json.dumps(content))

    			count=0
    			for i in a:
        			b=json.loads(i)
        			mydict={}
        			for j in ['business_id','name','city','state','full_address','latitude','longitude','stars']:
            				mydict[j]=b[j]
        			#print "db.business.insert( ",mydict," )"
				"""
				"""
				if (mydict['city']):
        				count+=1
					session.execute(
             				"""
                			INSERT INTO business_details (business_id,name,city,state,full_address,latitude,longitude,stars)
                			VALUES (%(business_id)s, %(name)s, %(city)s, %(state)s , %(full_address)s, %(latitude)s, %(longitude)s,%(stars)s)
                			""",mydict)
        				#setLog("INFO","Insert Done : "+ mydict['business_id']+"\t"+mydict['name'])
        				set_log("INFO", "logs", "Insert Done : "+ mydict['business_id']+"\t"+mydict['name'])
    			#setLog("INFO","Inserted "+str(count)+" number of businesses ")


	except Exception as e:
		#setLog("ERROR", " Failed inserting into business_details : " +str(e))
		setLog("ERROR", "debug", " Failed inserting into business_details : " +str(e))


def setLog(type_of_log,string):
        if type_of_log == "INFO":
         type_of_log = 'logs'
	"""Print DB Failures"""
	#with open("db_failure_logs.txt", "a") as myfile:
	ts = time.time()
	ts=datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
	#myfile.write("["+ts+"] : ["+type_of_log +"] :"+string+"\n")
	m = "[" + ts + "] : [" + type_of_log + "] :" + string
	try:
		print ("Sending :", m)
		if type_of_log == 'logs': producer.send('logs', m)
		if type_of_log == 'debug': producer.send('debug', m)
		print ("\nSent")			
	except:
		print ("Exception in sending to Kafka \n Check if Kafka Cluster working")
    #print ("["+ts+"] : ["+type_of_log +"] :"+string+"\n")
	#"""

	
if __name__=='__main__':
	#print get_food_details('xyz')
	#e={'food': u'sandwich', 'count': 4, 'business_id': u'fDC3yJqfHFq2bJ8D4F535w'}
	#insert_food_details(e,"Yelp")
	
	#insert_business_details(config.businesslist)

	#get_business_id("Bear Creek Golf Complex","Chandler")
        #get_business_details('Goodyear')
    get_top_restaurants('Tacos', 'Goodyear')
