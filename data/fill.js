var monk = require('monk');

var db = monk(require('../config'));
var R = require('ramda');
var fs = require('fs');
var dbName = require('../config');
dbName = dbName.split('/')[1];
function fillDb(_data, page, min) {
    var pagemetadata = db.get('pagemetadata');
    var pagefetchCol = db.get('pagefetch');
    var user = db.get('user');
    var coords = db.get('coords');
    var tag = db.get('tag');
    var data = _data;
    data.page = 1000 * min;
    data.page += page;
    if (!data) {
        return new Promise((res, rej) => {
            fs.appendFile(dbName + 'lastAdded', data.page + '\n', (e) => {
                if (e) rej(e);
                res();
            });
        });
    }
    var existing = pagefetchCol.findOne({ page: data.page });

    return existing.then(d => {
        var p = [];
        if (!d) {
            console.log('filled', data.page);
            p.push(pagefetchCol.insert({ page: data.page }));
            // user
            data.users.forEach((u, i) => {
                p.push(user.insert({ timestamp: data.timestamp, user: u, page: data.page, uid: data.uid[i] }))
            });
            data.tags.forEach(t => {
                p.push(tag.insert({ timestamp: data.timestamp, tag: t, page: data.page }));
            })
            data.coords.forEach(t => {
                p.push(coords.insert({ timestamp: data.timestamp, page: data.page, coord: t }));
            });
            p.push(pagemetadata.insert(R.omit(['user', 'coords', 'tags'], data)));
        } else {
            console.log('db hass it', data.page);
        }
        return Promise.all(p).then(x => {
            return new Promise((res, rej) => {
                fs.appendFile(dbName + 'lastAdded', data.page + '\n', (e) => {
                    if (e) rej(e);
                    res();
                });
                
            });
        });
    });
}

module.exports = fillDb;
