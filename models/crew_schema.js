
var mongoose = require("mongoose");

var crew = new mongoose.Schema({
    tconst: String,
    directors: String,
    writers: String,
});

var Crew = mongoose.model("Crew", crew);
module.exports = Crew;