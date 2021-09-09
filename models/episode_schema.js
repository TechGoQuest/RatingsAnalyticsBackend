
var mongoose = require("mongoose");

var episode = new mongoose.Schema({
    tconst: String,
    parentTconst: String,
    seasonNumber: String,
    episodeNumber: String
});

var Episode = mongoose.model("Episode", episode);
module.exports = Episode;