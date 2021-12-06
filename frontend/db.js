var mysql = require('mysql')

var connection = mysql.createConnection({
  host: 'localhost',
  user: 'Group3',
  password: 'Group3',
  database: 'usedCarsDB'
})

connection.connect(function(err) {
  if (err) throw err
  console.log('You are now connected...')
})

connection.query('SELECT * FROM ', function(err, results) {
    if (err) throw err
    console.log(results[0])
  })
