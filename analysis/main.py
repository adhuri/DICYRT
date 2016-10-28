import config
import sys

def getFoodlist():
    lines=[]
    with open(config.foodlist) as f:
        lines = f.read().splitlines()
    return lines

def search_query_1(food,location):
    print "\n===Query results 1 for food : ",food," and location : ",location,"==\n"

def search_query_2(restaurant,location):
    print "\n===Query results 2 for restaurant : ",restaurant," and location : ",location,"==\n"

    print "Making API call for google_places_kafka"
    """
    Make an API call here once you have the business id and name and location
    """


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
            search_query_2(restaurant,location)
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
