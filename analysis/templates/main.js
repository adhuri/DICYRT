function initMap() {
    var bounds = new google.maps.LatLngBounds();          
    map = new google.maps.Map(document.getElementById("map"));

    $.get('restaurantDetails.json', function(data) {
        var markers = data.restaurants
        for( i = 0; i < markers.length; i++ ) 
        {
            var marker = markers[i];
            var position = new google.maps.LatLng(marker.lat, marker.lng);
            bounds.extend(position);
            marker = new google.maps.Marker({
                position: position,
                map: map,   
                title: marker.restName
            });
            map.fitBounds(bounds);
        }
    }, 'json');

}