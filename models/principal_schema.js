var mongoose = require("mongoose");

var principal = new mongoose.Schema({
    tconst: String,
    ordering: String,
    nconst: String,
    category: String,
    job: String,
    characters: String,
});

var Principal = mongoose.model("Principal", principal);
module.exports = Principal;