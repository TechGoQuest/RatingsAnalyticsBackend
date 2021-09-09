var express = require("express");
const bodyParser = require('body-parser');
var mongoose = require("mongoose");
var config = require('config');
var cors = require('cors')
var { DATABASE_NAME } = require("./constants")
var app = express();
var port = config.app.port;

mongoose.connect(`mongodb://localhost:27017/${DATABASE_NAME}`, { useNewUrlParser: true }, (err, data) => { });

mongoose.Promise = global.Promise;

app.use(bodyParser.json());

app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
    extended: true
}));

app.use(cors({
    origin: ['http://139.59.16.11', 'http://localhost:3000', "http://159.65.152.164:5000", 'http://139.59.18.134:5000'],
    credentials: true
}));

app.use(function (req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

app.use(express.static(__dirname));

//routes
const user = require("./routes/user")
const analysis = require("./routes/analysis")
const country = require("./routes/country")
const language = require("./routes/language")
const setting = require("./routes/setting")
const emailalerts = require("./routes/emailalerts")
const activity = require("./routes/activity")

app.use("/user", user)
app.use("/analysis", analysis)
app.use("/country", country)
app.use("/language", language)
app.use("/setting", setting)
app.use("/emailalerts", emailalerts)
app.use("/activity", activity)

app.listen(port, () => {
    console.log("Server listening on Port- " + port);
});

config = config.get('app')
