
var mongoose = require("mongoose");

var name = new mongoose.Schema({
    nconst: String,
    primary_name: String,
    birth_year: String,
    death_year: String,
    primary_profession: String,
    known_for_title: String
});

var Name = mongoose.model("Name", name);
module.exports = Name;