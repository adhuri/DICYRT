import config
import sys
import cass

def getFoodlist():
    lines=[]
    with open(config.foodlist) as f:
        lines = f.read().splitlines()
    return lines

def search_query_1(food,location):
    cass.setLog("INFO","Query results 1 for food : "+food+" and city : "+location)
    b_id=cass.get_business_id(food,location)
    print b_id
    #print cass.get_food_details(b_id)

def search_query_2(restaurant,location):
    cass.setLog("INFO","Query results 2 for restaurant : "+restaurant+" and location : "+location)
    
    b_id=cass.get_business_id(restaurant,location)
    if b_id is None:
        return {}
    else:
        result= cass.get_food_details(b_id)
        return result	
        cass.setLog("INFO", "Making API call for google_places_kafka")


def get_top10_restaurant():
    foodList= getFoodlist()
    print ("FoodList : "+ ','.join(foodList))
    food=raw_input("Enter Food from the list given above : ")
    if (food in foodList):

        location=raw_input("Enter Location where you have to search : ")
        if location is not '':
            search_query_1(food,location)
        else:
            print ("Invalid Location \nERROR: Exiting..")
        sys.exit()
    else:
        print ( "Please Enter from the food list provided \nERROR: Exiting..")
        sys.exit()

def get_top10_food():
    restaurant=raw_input("Enter Restaurant Name : ")
    if (restaurant is not ''):
        location=raw_input("Enter Location where you have to search the restaurant : ")
        if location is not '':
            print search_query_2(restaurant,location)
        else:
            print ("Invalid Location \nERROR: Exiting..")
        sys.exit()
    else:
        print ( "Please Enter restaurant name \nERROR: Exiting..")
        sys.exit()





if __name__=="__main__":
    while (True):
        print ("1.  Get Top 10 restaurants(R) for food(F) in a location (L)")
        print ("2.  Get Top 10 food items (F) for Restaurant (R)")
        print ("3.  Quit")

        ins = int(raw_input("Please enter Option from above : "))
        if (ins == 1):
            get_top10_restaurant()
        elif (ins == 2):
            get_top10_food()
        elif (ins == 3):
            print "Thank You. \nExiting.."
        else :
            print "Enter Valid input"
            continue
