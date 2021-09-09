const express = require("express");
const Country = require("../models/country_schema");
const router = express.Router();
const Language = require("../models/language_schema");
const AKAS = require("./../models/akas_schema")

router.get("/list", (req, res) => {
    Language.find({}, { __v: 0, _id: 0 }).sort({ name: 1 }).exec((err, data) => {
        res.send({ 'status': 1, data })
    })
})

router.get("/extract", (req, res) => {
    var Languages = []
    AKAS.find({}, (err, akas) => {
        // console.log(akas);
        // akas.forEach(element => {
        //     Languages.push(element.language)
        // });
        res.send({ data: akas })
    }).limit(10)
})

router.post("/create", (req, res) => {
    var insert_arr = [
        // { "code": "af", "name": "Afrikaans" },
        // { "code": "ar", "name": "Arabic" },
        // { "code": "az", "name": "Azeri (Latin)" },
        // { "code": "be", "name": "Belarusian" },
        // { "code": "bg", "name": "Bulgarian" },
        // { "code": "ca", "name": "Catalan" },
        // { "code": "cs", "name": "Czech" },
        // { "code": "cy", "name": "Welsh" },
        // { "code": "da", "name": "Danish" },
        // { "code": "de", "name": "German" },
        // { "code": "dv", "name": "Divehi" },
        // { "code": "el", "name": "Greek" },
        // { "code": "en", "name": "English" },
        // { "code": "eo", "name": "Esperanto" },
        // { "code": "es", "name": "Spanish" },
        // { "code": "et", "name": "Estonian" },
        // { "code": "eu", "name": "Basque" },
        // { "code": "fa", "name": "Farsi" },
        // { "code": "fi", "name": "Finnish" },
        // { "code": "fo", "name": "Faroese" },
        // { "code": "fr", "name": "French" },
        // { "code": "gl", "name": "Galician" },
        // { "code": "gu", "name": "Gujarati" },
        // { "code": "he", "name": "Hebrew" },
        // { "code": "hi", "name": "Hindi" },
        // { "code": "hr", "name": "Croatian" },
        // { "code": "hu", "name": "Hungarian" },
        // { "code": "hy", "name": "Armenian" },
        // { "code": "id", "name": "Indonesian" },
        // { "code": "is", "name": "Icelandic" },
        // { "code": "it", "name": "Italian" },
        // { "code": "ja", "name": "Japanese" },
        // { "code": "ka", "name": "Georgian" },
        // { "code": "kk", "name": "Kazakh" },
        // { "code": "kn", "name": "Kannada" },
        // { "code": "ko", "name": "Korean" },
        // { "code": "kok", "name": "Konkani" },
        // { "code": "ky", "name": "Kyrgyz" },
        // { "code": "lt", "name": "Lithuanian" },
        // { "code": "lv", "name": "Latvian" },
        // { "code": "mi", "name": "Maori" },
        // { "code": "mk", "name": "FYRO Macedonian" },
        // { "code": "mn", "name": "Mongolian" },
        // { "code": "mr", "name": "Marathi" },
        // { "code": "ms", "name": "Malay" },
        // { "code": "mt", "name": "Maltese" },
        // { "code": "nb", "name": "Norwegian (Bokm?l)" },
        // { "code": "nl", "name": "Dutch" },
        // { "code": "ns", "name": "Northern Sotho" },
        // { "code": "pa", "name": "Punjabi" },
        // { "code": "pl", "name": "Polish" },
        // { "code": "ps", "name": "Pashto" },
        // { "code": "pt", "name": "Portuguese" },
        // { "code": "qu", "name": "Quechua" },
        // { "code": "ro", "name": "Romanian" },
        // { "code": "ru", "name": "Russian" },
        // { "code": "sa", "name": "Sanskrit" },
        // { "code": "se", "name": "Sami (Northern)" },
        // { "code": "sk", "name": "Slovak" },
        // { "code": "sl", "name": "Slovenian" },
        // { "code": "sq", "name": "Albanian" },
        // { "code": "sv", "name": "Swedish" },
        // { "code": "sw", "name": "Swahili" },
        // { "code": "syr", "name": "Syriac" },
        // { "code": "ta", "name": "Tamil" },
        // { "code": "te", "name": "Telugu" },
        // { "code": "th", "name": "Thai" },
        // { "code": "tl", "name": "Tagalog" },
        // { "code": "tn", "name": "Tswana" },
        // { "code": "tr", "name": "Turkish" },
        // { "code": "tt", "name": "Tatar" },
        // { "code": "ts", "name": "Tsonga" },
        // { "code": "uk", "name": "Ukrainian" },
        // { "code": "ur", "name": "Urdu" },
        // { "code": "uz", "name": "Uzbek (Latin)" },
        // { "code": "vi", "name": "Vietnamese" },
        // { "code": "xh", "name": "Xhosa" },
        // { "code": "zh", "name": "Chinese" },
        // { "code": "zu", "name": "Zulu" },

        { "code": "qbn", "name": "Flemish" },
        { "code": "cmn", "name": "Mandarin" },
        { "code": "qbp", "name": "Castilian" },
        { "code": "rn", "name": "Rundi" },
        { "code": "bs", "name": "Bosnian" },
        { "code": "yi", "name": "Yiddish" },
        { "code": "qbo", "name": "Serbo-Croatian" },
        { "code": "tg", "name": "Tajik" },
        { "code": "gsw", "name": "Swiss-German" },
        { "code": "yue", "name": "Cantonese" },
        { "code": "la", "name": "Latin" },
        { "code": "bn", "name": "Bengali" },
        { "code": "ml", "name": "Malayalam" },
        { "code": "gd", "name": "Gaelic" },
        { "code": "ga", "name": "Irish" },
        { "code": "qal", "name": "Creole" },
        { "code": "wo", "name": "Wolof" },
        { "code": "no", "name": "Norwegian" },
        { "code": "nqo", "name": "N'Ko" },
        { "code": "sd", "name": "Sindhi" },
        { "code": "ku", "name": "Kurdish" },
        { "code": "rm", "name": "Raeto-Romance" },
        { "code": "roa", "name": "Romance languages" },
        { "code": "su", "name": "Sundanese" },
        { "code": "jv", "name": "Javanese" },
        { "code": "prs", "name": "Dari" },
        { "code": "fro", "name": "French, Old" },
        { "code": "haw", "name": "Hawaiian" },
        { "code": "lo", "name": "Lao" },
        { "code": "my", "name": "Burmese" },
        { "code": "am", "name": "Amharic" },
        { "code": "qac", "name": "Aboriginal" },
        { "code": "ne", "name": "Nepali" },
        { "code": "myv", "name": "Erzya" },
        { "code": "br", "name": "Breton" },
        { "code": "iu", "name": "Inuktitut" },
        { "code": "st", "name": "Southern Sotho" },
        { "code": "cr", "name": "Cree" }
    ]
    Language.insertMany(insert_arr, (err, create) => {
        res.send({ 'status': 1, 'message': 'Done' })
    })
})

router.get("/option", (req, res) => {
    var L = []
    Country.find({}, (err, lang) => {
        lang.forEach(element => {
            L.push("<Option value = " + element.code + ">" + element.name + "</Option>")
        });
        res.send({ data: L })
    })
})

module.exports = router;