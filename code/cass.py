from cassandra.cluster import Cluster 
import operator
cluster = Cluster(
	contact_points=['152.46.19.234'],
	)
session = cluster.connect('aniket')


def get_food_details(b_id,top=10):
	global session
	get_result_prepared = session.prepare("select business_id,food,count from food_details where business_id=? ALLOW FILTERING")
	results= session.execute(get_result_prepared,[b_id])
	return getDict(results)[:top]


def getDict(results):
	dic={}
	for result in results:
		if result.food in dic:
			dic[result.food]+= result.count
		else:
			dic[result.food] = result.count
	#print dic
	sorted_x = sorted(dic.items(), key=operator.itemgetter(1),reverse=True)
	return sorted_x

if __name__=='__main__':
	print get_food_details('xyz')
