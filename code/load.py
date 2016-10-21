import json
import mongo_connect
from mongo_helper import select,insert
import sys

sys.dont_write_bytecode=True

DATA_FOLDER="/home/adhuri/DICYRT/data/"

def load_business(fname):
    with open(fname) as f:
        content = f.readlines()
        #print content
        a= json.loads(json.dumps(content))

    count=0
    for i in a:
	count+=1
        b=json.loads(i)
        mydict={}
        for i in ['business_id','name','city','state','full_address','latitude','longitude','stars']:
            mydict[i]=b[i]
        #print "db.business.insert( ",mydict," )"
	
	insert(db.business,mydict)
	print "Insert Done : ", mydict['business_id'],"\t",mydict['name']
    print "--------------------------------------------------------------------------\n Inserted ",count," number of business \n --------------------------------------------------------------------------\n"

if __name__=="__main__":
	global db
	db=mongo_connect.get_db()
	
	business_file=DATA_FOLDER+"yelp_academic_dataset_business.json"
	load_business(business_file)
