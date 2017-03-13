var fs = require('fs');

var walkPath = './osm2';
var processFile = require('./handleOsc');

var walk = function (dir, done) {
    fs.readdir(dir, function (error, list) {
        if (error) {
            return done(error);
        }

        var i = 0;

        (function next() {
            var file = list[i++];

            if (!file) {
                return done(null);
            }

            file = dir + '/' + file;

            fs.stat(file, function (error, stat) {

                if (stat && stat.isDirectory()) {
                    walk(file, function (error) {
                        next();
                    });
                }
                else {
                    // do stuff to file here
                    fs.readFile(file, function read(err, data) {
                        if (err) {
                            throw err;
                        }
                        processFile(Promise.resolve(data)).then(() => {
                            console.log('solved', file);
                        });
                        next();
                        
                    });
                }
            });
        }());
    });
};


console.log('-------------------------------------------------------------');
console.log('processing...');
console.log('-------------------------------------------------------------');

walk(walkPath, function (error) {
    if (error) {
        throw error;
    }
 else {
        console.log('-------------------------------------------------------------');
        console.log('finished.');
        console.log('-------------------------------------------------------------');
    }
});