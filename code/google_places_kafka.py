from google_places import get_place_details,get_place
from kafka import KafkaProducer
import json

# p = get_place('RockysLounge','40.3964688,-80.0849416')



def create_list_for_kafka(business_id,name,location):
	p=get_place(name,location)
	
	js= get_place_details(p)
	lists=[]
	for i in js['reviews']:
		 i['business.id']=business_id
		 lists.append(i)	
	return lists


def main(business_id, restaurant_name, location):
	try:
		producer = KafkaProducer(bootstrap_servers='152.46.16.173:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
		for i in create_list_for_kafka(business_id, restaurant_name, location):
			print ("Sending :", i)
			producer.send('google_places', i)
			print ("\nSent")
			
	except:
		print ("Exception in sending to Kafka \n Check if Kafka Cluster working")

if __name__=="__main__":
	main('ASidDajshf','RockysLounge','40.3964688,-80.0849416')
