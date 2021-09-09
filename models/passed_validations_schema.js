var mongoose = require("mongoose");

var passed_validations = new mongoose.Schema({
    startYear: String,
    tconst: String,
    titleType: String,
    averageRating: Array,
    numVotes: Array,
    alternateTitle: Array,
    genres: Array,
    region: Array,
    language: Array,
    originalTitle: String,
    main_language: String,
    main_country: String,
    primaryTitle: String,
});

var Passed_validations = mongoose.model("Passed_validations", passed_validations);
module.exports = Passed_validations;
