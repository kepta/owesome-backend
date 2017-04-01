var express = require('express');
var router = express.Router();
var moment = require('moment');
var mongo = require('mongodb');
var R = require('ramda');
var bboxtoPoly = require('turf-bbox-polygon');

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
    var dateTo = req.query.to;
    var dateFrom = req.query.from;
    var users = req.query.users && req.query.users.split(',');
    var tags = req.query.tags;
    var bbox = req.query.bbox && req.query.bbox.split(',');
    var query = {};
    var promises = [];
    if (dateTo && dateFrom) {
        var $to = moment(dateTo).startOf('hour')
            .toDate();
        var $from = moment(dateFrom).startOf('hour')
            .toDate();

        query.timestamp = {
            $lte: $to,
            $gte: $from
        }
        console.log($to, $from);
    }
    if (tags) {
        const q = Object.assign({
            tag: {
                $in: tags.split(',')
            }
        }, query
        );
        console.log(q);
        
        const collection = db.get('tag');
        promises.push(collection.find(q, ['page']).then(docs => docs.map(d => d.page)));
    }

    if (users) {
        const q = Object.assign({
            user: {
                $in: users
            }
        }, query)
        console.log(q);
        const collection = db.get('user');
        promises.push(collection.find(q, ['page']).then(docs => docs.map(d => d.page)));
    }

    if (bbox) {
        bbox = bbox.map(n => parseFloat(n));
        const q = Object.assign({
            coords: {
                $geoWithin: {
                    $geometry: bboxtoPoly(bbox).geometry
                }
            }
        }, query);
        console.log('check', q, JSON.stringify(bboxtoPoly(bbox).geometry, null, 2), 'check2');
        const collection = db.get('coords');
        promises.push(collection.find(q, ['page']).then(docs => docs.map(d => d.page)));
    }

    Promise.all(promises).then(r => {
        var result = r.reduce((s, n) => R.intersection(s, n), r[0]);
        res.json({ len: result.length, docs: result });
    })
    // db.pagemetadata.find({ coords: {$geoWithin:{ $geometry:  {  "type": "Polygon","coordinates": [ [ [   -18.6328125,   43.58039085560784 ], [   46.05468749999999,   43.58039085560784 ], [   46.05468749999999,   62.2679226294176 ], [   -18.6328125,   62.2679226294176 ], [   -18.6328125,  43.58039085560784 ] ] ]}     } }} )


    // var collection = db.get('pagemetadata');
    
    // console.log(query);
    // collection.find(query, ['page', 'timestamp']).then(docs => {
    //     res.json({ len: docs.length , docs });
    // })
    // .catch(console.error);

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