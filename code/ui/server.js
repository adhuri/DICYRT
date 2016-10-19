var http = require('http');
var fs = require('fs');
var formidable = require("formidable");
var util = require('util');
const cassandra = require('cassandra-driver');
const client = new cassandra.Client({ contactPoints: ['152.46.19.234'], keyspace: 'kspace'});
const query = 'SELECT * from business';

var server = http.createServer(function (req, res) {
    if (req.method.toLowerCase() == 'get') {
        displayForm(res);
    } else if (req.method.toLowerCase() == 'post') {
        processAllFieldsOfTheForm(req, res);
    }

});

function displayForm(res) {
    fs.readFile('form.html', function (err, data) {
        res.writeHead(200, {
            'Content-Type': 'text/html',
                'Content-Length': data.length
        });
        res.write(data);
        res.end();
    });
}

function processAllFieldsOfTheForm(req, res) {
    var form = new formidable.IncomingForm();

    form.parse(req, function (err, fields, files) {
        //Store the data from the fields in your data store.
        //The data store could be a file or database or any other store based
        //on your application.
        client.execute(query, function (err, result) {
  		var user = result.first();
  		//The row is an Object with column names as property keys.
  		// res.write(user.business_id);
    //     res.write(user.name);
    //     res.write(user.city);
    //     res.write(user.full_address);
    	console.log(user);
  		});
        res.writeHead(200, {
            'content-type': 'text/plain'
        });
        res.write('received the data:\n\n');
        
        res.end(util.inspect({
            fields: fields,
            files: files
        }));
    });
}

server.listen(1185);
console.log("server listening on 1185");
