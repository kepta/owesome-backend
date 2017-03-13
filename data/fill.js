var monk = require('monk');

var db = monk(require('../config'));
var R = require('ramda');

function fillDb(_data, page, min) {
    var collection = db.get('pagemetadata');
    var data = _data;
    data.page = 1000 * min;
    data.page += page;
    var existing = collection.findOne({ page: data.page });
    return existing.then(d => {
        if (!d) {
            return collection.insert(data);
        }
        return null;
    });
}

module.exports = fillDb;
