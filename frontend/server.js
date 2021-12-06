const express = require("express");
const events = require('events');
var bodyParser = require('body-parser');

const app = express();
const ports = 8080;

// global variables 

var limit = 50;
var i = 25;
var Search = ''
//

app.use(express.static("public"));
app.use(bodyParser.urlencoded({extended:true}));

app.engine('html', require('ejs').renderFile)

app.listen(ports, () => 
	console.log("Server is running on port 8080")
);

const { Client } = require('pg')
const client = new Client({
  	user: 'group3',
  	host: 'localhost',
  	database: 'usedCars',
  	password: 'group3'
})

client.connect(function(err){
		if (err) throw err
		console.log("You have connected succesfully...")
	})


app.get('/', (req, res, next)=>{

		sql = "SELECT * FROM Predicted_Prices_FINAL ORDER BY make ASC LIMIT " + limit.toString(); 

		console.log(sql)

		//query to the DB
		client.query(sql,function(err, result){
			if(err) throw err
			//console.log(result)
			res.render(__dirname + "/views/test.html" , {title: "Predicted Values", data: result.rows})
		})

});
app.post('/',(req,res) => {

if(req.body.loadMore!=null){
	limit += 25;
	console.log(limit);
	sql = ""

	if(Search != ''){
		console.log(Search)
		sql = "SELECT * FROM Predicted_Prices_FINAL WHERE Model = \'" + Search +  "\' ORDER BY make ASC LIMIT " + limit.toString();
	}
	else
		sql = "SELECT * FROM Predicted_Prices_FINAL ORDER BY make ASC LIMIT " + limit.toString();

	console.log(sql)

	//query to the DB
        client.query(sql,function(err, result){
        if(err) throw err
                res.render(__dirname + "/views/test.html" , {title: "Predicted Values", data: result.rows})
	})
}

if(req.body.loadNum!=null) {
	console.log(req.body.loadNum)
	if(req.body.loadNum === '')
		limit += 25 
	else
		limit = parseInt(req.body.loadNum);
	
	console.log(limit);

        if(req.body.Model_search != '' && req.body.Model_search != 'Search Car Model...'){
		Search = req.body.Model_search
                sql = "SELECT * FROM Predicted_Prices_FINAL WHERE Model = \'" + Search +  "\' ORDER BY make ASC LIMIT " + limit.toString();
	}
	else
       		sql = "SELECT * FROM Predicted_Prices_FINAL ORDER BY make ASC LIMIT " + limit.toString();
       
	console.log(sql)
 
        //query to the DB
        client.query(sql,function(err, result){
        if(err) throw err
                res.render(__dirname + "/views/test.html" , {title: "Predicted Values", data: result.rows})
        })
}

});
