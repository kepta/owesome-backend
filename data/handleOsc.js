const fetch = require('node-fetch');
const pako = require('pako');
const R = require('ramda');
const xtoj = require('xml2js').parseString;
const gimme = R.curry((s, d) => R.pluck(s, d).filter(R.identity));
const fillDb = require('./fill');
var moment = require('moment');
var monk = require('monk');
var events = require('events');
var fs = require('fs');
var db = monk(require('../config'));

function toPoint(lon, lat) {
    if (!lon || !lat) return null;
    return {
        type: "Point",
        coordinates: [parseFloat(parseFloat(lon).toFixed(2)), parseFloat(parseFloat(lat).toFixed(2))]
    };
}

function getMetaData(obj) {
    if (!obj) return null;
    const addTo = R.curry((t, $) =>
        R.compose(
            R.uniq, R.filter(R.identity), R.pluck(t)
        )($)
    );
    const addToInt = R.curry((t, $) => addTo(t, $).map(i => parseInt(i, 10)));
    const addGeoPoints = ($) => {
        const array = $.map(x => toPoint(x.lon, x.lat))
            .filter(R.identity)
            .map(JSON.stringify);
        return R.uniq(array).map(JSON.parse);
    };
    const $ = R.pluck('$', obj);
    let tags = R.compose(
        R.map(x => [x.k, x.v]), R.pluck('$'), R.unnest, R.filter(R.identity), R.pluck('tag')
    )(obj);
    const keys = R.uniq(R.map(R.head, tags));

    // fill keys=*
    keys.forEach(k => {
        tags.push([k, '*'])
    });

    // convert them to k=v format
    tags = R.uniq(tags.map(t => t[0] + '=' + t[1]));

    const members = R.compose(R.pluck('$'), R.unnest, R.filter(R.identity), R.pluck('member'))(obj);
    const memberPluck = R.curry((pluck, memb) => R.compose(R.uniq, R.filter(R.identity), R.pluck(pluck))(memb));
    const wayNd = R.compose(R.pluck('ref'), R.pluck('$'), R.unnest, R.filter(R.identity), R.pluck('nd'))(obj)

    return {
        wayNd,
        users: addTo('user', $),
        uid: addToInt('uid', $),
        timestamp: moment(obj[0].$.timestamp).startOf('hour')
                    .toDate(),
        tags,
        nwr: R.countBy(R.identity, R.pluck('nwr', $)),
        membersType: memberPluck('type', members),
        membersRole: memberPluck('role', members),
        membersRef: memberPluck('ref', members),
        id: addToInt('id', $),
        coords: addGeoPoints($),
        changeset: addToInt('changeset', $),
        cdm: R.countBy(R.identity, R.pluck('cdm', $))
    };
}

const flattenIt = R.curry(d => R.unnest(R.unnest(R.compose(R.map(R.values), R.values)(d))));
const concatAll = R.unapply(R.reduce(R.concat, []));


function newDigest(r) {
    const pick = (x, y) => R.compose(
        R.map((i) => {
            if (!i) {
                return null;
            }
            i.$.nwr = x;
            i.$.cdm = y;
            return i;
    }), R.filter(R.identity), R.unnest, gimme(x));

    const newExtractNWR = (rron, cdm) => {
        if (!rron) return [];
        return concatAll(pick('node', cdm)(rron), pick('way', cdm)(rron), pick('relation', cdm)(rron));
    };
    var result = concatAll(
        newExtractNWR(r.osmChange.create, 'create'),
        newExtractNWR(r.osmChange.modify, 'modify'),
        newExtractNWR(r.osmChange.delete, 'delete')
    );
    return result;
}

function networkGet(_n, p) {
    var n = _n
    if (_n < 10) n = '0' + n
    if (_n < 100) n = '0' + n
    return fetch(`https://s3.amazonaws.com/osm-changesets/minute/002/${p}/${n}.osc.gz`)
        .then((d) => d.buffer());
}

function processFile(bufferProm, page, min, file) {
    return bufferProm
        .then(d => pako.inflate(d, { to: 'string' }))
        .then(d => new Promise((res, rej) => xtoj(d, { async: true }, (er, result) => {
            if (er) rej(er);
            return res(result);
        })))
        .then(newDigest)
        .then(getMetaData)
        .then(r => fillDb(r, page, min, file));
}


var ev = new events.EventEmitter();

function split(n) {
    return [parseInt(n / 1000, 10), n % 1000];
}

class Worker {
    constructor(id, queue, onComplete, file) {
        this.queue = queue || [];
        this.id = id;
        this.errors = [];
        this.currentPage = -1;
        this.errortry = 0;
        this.onComplete = onComplete;
        this.file = file;
        ev.addListener(`worker${id}`, (data) => {
            this.giveWork();
        });
        this.pagefetchDb = db.get('pagefetch');
        this.giveWork();
    }
    work(page) {
        var [minute, r] = split(page);
        this.pagefetchDb.findOne({ page })
        .then(d => {
            if (d) {
                console.log('db has', page);
                return new Promise((res, rej) => {
                    fs.appendFile(this.file, page + '\n', (e) => {
                        if (e) rej(e);
                        res();
                    });

                });
                return null;
            }
            return processFile(networkGet(r, minute), r, minute, this.file);
        })
        .then(() => {
            ev.emit(`worker${this.id}`, page);
        })
        .catch(e => {
            this.errors.push(page);
            console.log(page, e);
            ev.emit(`worker${this.id}`, -1);
        })
    }
    giveWork() {
        var page = this.queue.pop();
        if (page) {
            console.log('starting', page);
            this.currentPage = page;
            this.work(page);
        }
        else {
            this.currentPage = -1;
            console.log('FIN' + this.id + 'finished stuff', this.errors);
            this.onComplete(this.errors);
        }
    }
}


module.exports = Worker;
