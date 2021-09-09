
var mongoose = require("mongoose");

var title = new mongoose.Schema({
    tconst: String,
    titleType: String,
    primaryTitle: String,
    originalTitle: String,
    isAdult: Boolean,
    startYear: Number,
    endYear: Number,
    runtimeMinutes: String,
    genres: Array
});

title.index({ tconst: 1 });

var Title = mongoose.model("Title", title);

module.exports = Title;