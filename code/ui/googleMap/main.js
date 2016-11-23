function initMap() {
    var bounds = new google.maps.LatLngBounds();          
    map = new google.maps.Map(document.getElementById("map"));

    var markers = [
        {name:'Rocky\'s lounge', lat: 40.3964688, lng: -80.0849416}
    ];

    for( i = 0; i < markers.length; i++ ) {
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
}