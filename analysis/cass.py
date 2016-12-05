from cassandra.cluster import Cluster 
import operator
import datetime,time
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import config,json

cluster = Cluster(
	contact_points=['152.46.19.234'],
	)
session = cluster.connect('aniket')

producer = KafkaProducer(bootstrap_servers='152.46.16.173:9092',value_serializer=lambda v: v.encode('utf-8'))

def get_food_details(b_id,top=10):
	global session
	setLog("INFO","Getting food_details for business_id = " + b_id +", count "+str(top))
	get_result_prepared = session.prepare("select business_id,food,count from food_details where business_id=? ")
	results= session.execute(get_result_prepared,[b_id])
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

def insert_food_details(element,source):
	try:	
		element['source']=source
		setLog("INFO", " Inserting into food_details :" + str(element))
		session.execute(
		"""
    		INSERT INTO food_details (business_id, food, count, source)
    		VALUES (%(business_id)s, %(food)s, %(count)s, %(source)s)
    		""",element)
	except:
		setLog("ERROR", " Failed inserting into food_details :" + str(element))


def get_business_id(name,city):
	setLog("INFO","Getting  business_id for name : " + name +", city :  "+city)
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
        				setLog("INFO","Insert Done : "+ mydict['business_id']+"\t"+mydict['name'])
    			#setLog("INFO","Inserted "+str(count)+" number of businesses ")


	except Exception as e:
		setLog("ERROR", " Failed inserting into business_details : " +str(e))

def setLog(type_of_log,string):
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

	get_business_id("Bear Creek Golf Complex","Chandler")
