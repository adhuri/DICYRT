function initMap() {
    var bounds = new google.maps.LatLngBounds();          
    map = new google.maps.Map(document.getElementById("map"));

    $.get('restaurantDetails.json', function(data) {
        // console.log(data.restaurants)
        var markers = data.restaurants
        for( i = 0; i < markers.length; i++ ) 
        {
            var marker = markers[i];
            var position = new google.maps.LatLng(marker.lat, marker.lng);
            bounds.extend(position);
            marker = new google.maps.Marker({
                position: position,
                map: map,   
                title: marker.name
            });
            map.fitBounds(bounds);
        }
    }, 'json');

    // var markers = [
    //     ['Rocky\'s lounge', 40.3964688, -80.0849416]
    // ];

    // for( i = 0; i < markers.length; i++ ) {
    //     var marker = markers[i];
    //     var position = new google.maps.LatLng(marker[1], marker[2]);
    //     bounds.extend(position);
    //     marker = new google.maps.Marker({
    //         position: position,
    //         map: map,
    //         title: marker[0]
    //     });
    //     map.fitBounds(bounds);
    // }
}