from google_places import get_place_details,get_place


# p = get_place('RockysLounge','40.3964688,-80.0849416')



def create_list_for_kafka(business_id,name,location):
	p=get_place(name,location)
	
	js= get_place_details(p)
	lists=[]
	for i in js['reviews']:
		 i['business.id']=business_id
		 lists.append(i)	
	return lists



if __name__=="__main__":
	for i in create_list_for_kafka('ASidDajshf','RockysLounge','40.3964688,-80.0849416'):
		print (i)
