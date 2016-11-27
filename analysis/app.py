from flask import Flask,redirect, url_for, request
import unicodedata,main
from flask import Markup
from flask import Flask
from flask import render_template
app = Flask(__name__)

@app.route("/searchrestaurant")
def searchR():
    return render_template('searchrestaurant.html')

@app.route("/restaurant" , methods = [ 'GET'])
def searchrestaurant():
    if request.method == 'GET':
        food = request.args.get('food')
        city=request.args.get('city')
    j={}
    
    
    
    return render_template('restaurant.html')

@app.route("/maps")
def searchMaps():
    restaurant_data = {"restaurants":[
    {"restName":"London Eye, London", "lat": 51.503454, "lng": -0.119562, "address": "Address1", "rating" : 4},
    {"restName":"Palace of Westminster, London", "lat": 51.499633, "lng": -0.124755, "address": "Address2", "rating" : 5}]};
    return render_template('map.html', restaurant_data=restaurant_data)
 
@app.route("/searchfood")
def searchF():
    return render_template('searchfood.html')


@app.route("/food",methods = [ 'GET'])
def chart():
    """{'business_id': u'jRPtR43eLXJmnr9Mw_deMg', 'food_list': [(u'sandwich', 15), (u'fish', 3), (u'chicken', 2)]}"""
    #j={'business_id': u'jRPtR43eLXJmnr9Mw_deMg', 'food_list': [(u'sandwich', 15), (u'fish', 3), (u'chicken', 2)]}
    #j=search_query_2(,)

    if request.method == 'GET':
    	restaurant = request.args.get('name')
    	city=request.args.get('city')
    	j={}
	j=main.search_query_2(restaurant,city)
	if bool(j):
		title="Food at "+ restaurant
    		labels =  [list([x[0].encode('ascii','ignore'),x[1]]) for x in j['food_list']]
    		print labels
    		return render_template('food.html', title=title,labels=labels)
	#else: return error 



if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)
