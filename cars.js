const express = require("express");
const https = require("https");
const bodyParser = require("body-parser");
var fs = require("fs");


const app = express();

app.use(bodyParser.urlencoded({extended:true}));
app.use(express.urlencoded({extended: false}));

//local server
app.listen(3000,function(){
	console.log("Server is running on port 3000.");
});

//home page
app.get("/",function(req,res) {
	res.sendFile(__dirname + "cars.html");
});
