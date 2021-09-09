var mongoose = require("mongoose");

var rating = new mongoose.Schema({
    tconst: String,
    averageRating: Number,
    numVotes: Number,
});

rating.index({ tconst: 1 });

var Rating = mongoose.model("Rating", rating);

module.exports = Rating;

// http://128.199.16.58/basecamp-concord-project/Api/agreements