var express = require('express');
var router = express.Router();
var moment = require('moment');
var mongo = require('mongodb');

/* GET home page. */
router.get('/', function(req, res) {
    res.render('index', { title: 'Express' });
});
router.get('/userlist', function(req, res) {
    var db = req.db;
    var collection = db.get('usercollection');
    collection.find({},{},function(e,docs){
        res.json( docs);
    });
});

router.get('/page', function(req, res) {
    var db = req.db;
    var collection = db.get('pagemetadata');
    var dateTo = req.query.to;
    var dateFrom = req.query.from;
    var users = req.query.users && req.query.users.split(',');
    // var tags = req.query.tags || false;
    var bbox = req.query.bbox && req.query.bbox.split(',');
    var query = {};

    if (bbox) {
        query.lat = { $elemMatch: { $lte: parseFloat(bbox[3]), $gte: parseFloat(bbox[1]) } };
        query.lon = { $elemMatch: { $lte: parseFloat(bbox[2]), $gte: parseFloat(bbox[0]) } };
    }
    // db.pagemetadata.find({ coords: {$geoWithin:{ $geometry:  {  "type": "Polygon","coordinates": [ [ [   -18.6328125,   43.58039085560784 ], [   46.05468749999999,   43.58039085560784 ], [   46.05468749999999,   62.2679226294176 ], [   -18.6328125,   62.2679226294176 ], [   -18.6328125,  43.58039085560784 ] ] ]}     } }} )

    if (dateTo && dateFrom) {
        var $to = moment(dateTo).startOf('hour')
        .toDate();
        var $from = moment(dateFrom).startOf('hour')
        .toDate();
        console.log('here', dateTo,$to, dateFrom);
        
        query.timestamp = {
           $lte: $to,
           $gte: $from
        }
        console.log($to, $from);
    }
    if (users) {
        query.user = {
            $in: users
        };
    }
    console.log(query);
    collection.find(query, ['page', 'timestamp']).then(docs => {
        res.json({ len: docs.length , docs });
    })
    .catch(console.error);

});

router.post('/adduser', function(req, res) {
    var db = req.db;
    // Get our form values. These rely on the "name" attributes
    var user = req.body.user;
    var page = req.body.page;
    var collection = db.get('usercollection');
    collection.insert({
       user, page
    }, function (err, doc) {
        if (err) {
            // If it failed, return error
            res.send("There was a problem adding the information to the database.");
        }
        else {
            // And forward to success page
            res.redirect("userlist");
        }
    });
});
module.exports = router;