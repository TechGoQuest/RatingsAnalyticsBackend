var mongoose = require("mongoose");

var failed_validations = new mongoose.Schema({
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
    issues_in: Array
});

var Failed_validations = mongoose.model("Failed_validations", failed_validations);
module.exports = Failed_validations;
