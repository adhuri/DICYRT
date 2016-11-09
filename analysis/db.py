#import config

#import couchdb
#couch = couchdb.Server(config.couchdb_ip)
#food_counts_db = couch.create('foodcounts')
#food_counts_db = couch['foodcounts']

'''
def save_in_db(businessid_food_count_list):
    for element in businessid_food_count_list:
        #food_counts_db.save(element)
        print element
'''

def save_element_in_db(element):
    #food_counts_db.save(element)
    #print element
    details = get_business_details(element['business_id'])
    details['food'] = element['food']
    
    details['count'] = element['count']
    print details
    
def get_business_details(business_id):
    #get details from elastic search db
    return {'business_id': 'b1','name':'rest1','city':'raleigh','state':'nc','full_address':'2516 avent ferry road','latitude':20,'longitude':20,'stars':4}

