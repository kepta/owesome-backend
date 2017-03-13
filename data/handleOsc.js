const fetch = require('node-fetch');
const pako = require('pako');
const R = require('ramda');
const xtoj = require('xml2js').parseString;
const gimme = R.curry((s, d) => R.pluck(s, d).filter(R.identity));
const fillDb = require('./fill');
var moment = require('moment');
var mongod = require('mongodb');
var monk = require('monk');

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
        user: addTo('user', $),
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

const extractNWR = (rron) => ({
    way: R.unnest(R.unnest(R.map(gimme('way'))(rron))),
    relation: R.unnest(R.unnest(R.map(gimme('relation'))(rron))),
    node: R.unnest(R.unnest(R.map(gimme('node'))(rron)))
});

const extractCDM = r => ({
    modify: extractNWR(gimme('modify', r)),
    delete: extractNWR(gimme('delete', r)),
    create: extractNWR(gimme('create', r))
});

const bgMp = R.forEachObjIndexed((cdm, key1) => R.forEachObjIndexed((nwr, key2) => {
        nwr.forEach(m => {
        m.$.nwr = key2;
        m.$.cdm = key1;
})
}, cdm));
const flattenIt = R.curry(d => R.unnest(R.unnest(R.compose(R.map(R.values), R.values)(d))));

function digest(r) {
    // return r;
    var osmChange = gimme('osmChange')(r);
    var bgmp = bgMp(extractCDM(osmChange));
    var flatten = flattenIt(bgmp);
    return flatten;
}
function networkGet(_n, p) {
    var n = _n
    if (_n < 10) n = '0' + n
    if (_n < 100) n = '0' + n
    return fetch(`https://s3.amazonaws.com/osm-changesets/minute/002/${p}/${n}.osc.gz`)
        .then((d) => d.buffer());
}

function processFile(bufferProm, page, min) {
    return bufferProm
        .then(d => pako.inflate(d, { to: 'string' }))
        .then(d => new Promise((res, rej) => xtoj(d, { async: true }, (er, result) => {
            if (er) rej(er);
            return res(result);
        })))
        .then(d => digest([d]))
        .then(getMetaData)
        .then(r => fillDb(r, page, min))
}

function ranger(pageMax, minuteMin, startMinute) {
    var collection = db.get('pagemetadata');

    function miniRanger(minute, t) {
        if (minute < minuteMin) return Promise.resolve();
        function gooMin(x, y) {
            if (y >= pageMax) {
                return Promise.all(R.range(x, pageMax).map((r) => {
                    processFile(networkGet(r, minute), r, minute);
                }))
                .then(() => miniRanger(minute - 1, t))
                    .catch((e) => {
                        console.log(e);
                        return miniRanger(minute - 1, t);
                    });
            }
            return Promise.all(R.range(x, y + 1).map((r) => {
                var _page = 1000 * minute;
                _page += r;
                return collection.findOne({ page: _page }).then(d => {
                    if (d) {
                        return null;
                    }
                    console.log('doesnt has', r, minute);
                    
                        return processFile(networkGet(r, minute), r, minute);
                    });
                }))
                .then(r => gooMin(y, y + t))
                .catch(e => {
                    console.log(e);
                    return gooMin(x, y);
                });
        }
        return gooMin(0, t);
    }
    return miniRanger(startMinute, 5);
}

ranger(1000,process.env.PAGE_MIN || 340, process.env.PAGE_MAX || 352 );

module.exports = processFile;