const MongoClient = require('mongodb').MongoClient

MongoClient.connect('link-to-mongodb', (err, database) => {
  // ... start the server
})