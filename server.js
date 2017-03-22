const express = require('express');
const app = express();
const PORT = 5000;
var logger = require('morgan');
var bodyParser = require('body-parser');
var cors = require('cors');
var routes = require('./routes/index');
var monk = require('monk');
var db = monk(require('./config'));

app.use(cors())
app.use(logger('dev'));

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(function(req,res,next){
    req.db = db;
    next();
});

app.listen(PORT, function() {
  console.log('listening on ' + PORT)
});

app.get('/', function (request, response) {
  // do something here
    response.send('Hello World')
});

app.use('/', routes);

app.use(function(req, res, next) {
    var err = new Error('Not Found');
    err.status = 404;
    next(err);
});

if (app.get('env') === 'development') {
    app.use(function(err, req, res, next) {
        res.status(err.status || 500);
        res.render('error', {
            message: err.message,
            error: err
        });
    });
}

module.exports = app;