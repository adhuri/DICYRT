import sys,cass,config

sys.dont_write_bytecode=True


if __name__=="__main__":
	cass.insert_business_details(config.businesslist)

