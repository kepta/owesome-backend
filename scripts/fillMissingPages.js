var monk = require('monk');
var db = monk(require('../config'));
var dbName = require('../config');
dbName = dbName.split('/')[1];
var fs = require('fs');
var R = require('ramda');
var fileName = 'scripts/data/' + dbName + '-notFound.txt'
var Worker = require('../data/handleOsc');

function fill() {
    var pages = fs.readFileSync(fileName, 'utf-8');
    pages = pages.split('\n')
    .map(r => parseInt(r, 10))
    .filter(x => Number.isInteger(x))
    .sort((a, b) => a - b);
    var filled = 0;
    function onComplete() {
        return function (e) {
            if (e.length > 0) {
                console.log(e);
            }
            filled++;
            if (filled === 2) {
                process.exit(0);
            }
        }
    } 
    var filledFileName = 'scripts/data/' + dbName + '-filled-notFound.txt'
    // console.log(pages);
    new Worker(1, pages.slice(0, parseInt(pages.length / 2, 10)), onComplete(), filledFileName);
    new Worker(1, pages.slice(parseInt(pages.length / 2, 10), pages.length), onComplete(), filledFileName);
}

fill();