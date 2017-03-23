var fs = require('fs');
var dbName = require('../config');
dbName = dbName.split('/')[1];
var Worker = require('../data/handleOsc');

// @NOTE 352373 beyond this has delete bug fixed ie R.concat third arg fixed
var fileName = 'scripts/data/' + dbName + '-cronMinutely.txt'
var file = fs.readFileSync(fileName, 'utf-8');
file = file.split('\n').filter(x => x !== '')
    .map(x => parseInt(x, 10))
    .filter(x => Number.isInteger(x))
    .sort((a,b) => a - b);
file = file[file.length - 1];
file += 1;
function onComplete(e) {
    if (e.length > 0) {
        console.log(e);
    }
    process.exit(0);
}
new Worker(1, [file],onComplete, fileName);
