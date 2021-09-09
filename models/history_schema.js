
var mongoose = require("mongoose");

var history = new mongoose.Schema({
    titleId: String,
    rating: String,
    votes: String,
    created_on: Date,
    difference_in_rating: String,
    difference_in_votes: String
});

var History = mongoose.model("History", history);
module.exports = History;