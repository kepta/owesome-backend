var monk = require('monk');

var db = monk(require('../config'));
var R = require('ramda');

function fillDb(type) {
    var collection = db.get('pagemetadata');
    var collectionType = db.get(type);
    collection.find().forEach(doc => {
        doc.changeset.forEach(c => {
            collectionType.insert({
                [type]: c
            });
        });
        console.log('succ', doc.page);
    });
}
fillDb('changeset');

module.exports = fillDb;
