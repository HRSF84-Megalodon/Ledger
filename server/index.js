if (process.env.NODE_ENV !== 'production') {
  require('dotenv').load();
}
var express = require('express');
var bodyParser = require('body-parser');
// var db = require('../database-mongo');
var path = require('path');



var app = express();

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS, PUT, DELETE',
  'Access-Control-Allow-Headers': 'X-Requested-With,content-type',
  'Access-Control-Allow-Credentials': 'true',
  'Content-Type': 'text/plain'
}


//app.use(express.static(__dirname + '/../react-client/dist'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));


var port = process.env.PORT || 6000;

app.listen(port, function() {
  console.log(`listening on port ${port}!`);
});




