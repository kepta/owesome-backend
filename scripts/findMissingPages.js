var monk = require('monk');
var db = monk(require('../config'));
var dbName = require('../config');
dbName = dbName.split('/')[1];
var fs = require('fs');
var R = require('ramda');

function findPages(lowerLimit) {
    var pagemetadata = db.get('pagemetadata');
    var upperLimit = fs.readFileSync(dbName + 'lastAdded', 'utf-8');
    upperLimit = upperLimit.split('\n').filter(x => x !== '');
    upperLimit = parseInt(upperLimit[upperLimit.length - 1]);
    const expectedPages = R.range(lowerLimit, upperLimit);
    pagemetadata.find({}, ["page"]).then(R.pluck('page')).then(r => {
        return R.difference(expectedPages, r).sort((a,b) => a - b);
    })
    .then((r) => {
         fs.writeFileSync('scripts/data/' + dbName + 'notFoundBro.txt', R.reduce((s, i) => s + i + '\n', '', r), (e) => {
            if (e) rej(e);
            res('done');
        });
    })
    .then(() => process.exit(0));
}
findPages(250000);