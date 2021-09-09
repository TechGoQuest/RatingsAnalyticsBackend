const express = require("express");
const router = express.Router();
var formidable = require("formidable");
var shell = require("shelljs");
var fs = require("fs");
TSV = require('tsv')
var common_library = require("./../common");
var d3 = require("d3-dsv");
var Rating = require("../models/rating_schema");
const Title = require("../models/title_schema");
const Analysis = require("../models/analysis");
const AKAS = require("../models/akas_schema");
const Merge = require("../models/merge_schema");
const zlib = require('zlib');
const History = require('../models/history_schema');
const Cron_table = require("../models/cron_table_schema");
const Name_IMDb = require("../models/name_schema");
var http = require("https")
const config = require("config")
var filepath = config.get("path").filepath
var moment = require("moment");
const Newmerge = require("../models/newmerge_schema");
const Country = require("../models/country_schema");
const Language = require("../models/language_schema");
const Setting = require("../models/settings_schema");
const Margin_of_1 = require("../models/margin_of_1_schema")
const Above_6 = require("../models/above_6_schema")
const Below_6 = require("../models/below_6_schema")
const Above_500_votes = require("../models/above_500_votes_schema")


router.get("/hitting", (req, res) => {
    console.log("HITTING", moment(new Date()).format("LLLL"));
})

router.post("/", (req, res) => {
    console.log("start");
    var oldpath = "";
    var form = new formidable.IncomingForm({ maxFileSize: 1000 * 1024 * 1024 });
    var path = ({ "filepath": "/var/www/html/imdb/uploads/" })
    form.parse(req, function (err, fields, files) {
        if (err) {
            res.send({ "status": 0, "message": err.stack });
        } else {
            console.log("reached here ");
            if (fields.data_type) { // 1 = title, 2 = akas, 3 = name, 4 = crew, 5 = episode, 6 = principals, 7 = ratings
                if (files.zip_file) {
                    if (files.zip_file.type != "application/gzip") {
                        res.send({ "status": 0, "message": "Please choose a zipped file" });
                    } else {
                        oldpath = files.zip_file.path;
                        fileName = files.zip_file.name;
                        console.log("OLd Path", oldpath);
                        console.log("New Path", fileName);
                        var newFileName = new Date().getHours() + new Date().getMinutes() + new Date().getMilliseconds() + files.zip_file.name;
                        newpath = path.filepath;
                        console.log("the path of developement", newpath);
                        insertPath = newpath + newFileName;
                        console.log("inserting path is ", insertPath);
                        if (!fs.existsSync(newpath)) {
                            console.log("zip_file not exist");
                            shell.mkdir("-p", newpath);
                        }
                        common_library.unzip(oldpath, insertPath, fields.data_type, (response, message) => {
                            res.send({ 'status': response, message })
                        })
                    }
                } else {
                    res.send({ "status": 0, "message": "Please choose a zipped file." })
                }
            } else {
                res.send({ 'status': 0, 'message': 'Please enter imdb data type' })
            }
        }
    });
})

router.post("/organalyze", (req, res) => {
    var ID = []
    var Ratings = []
    var Votes = []
    var finalData = []
    Rating.find({ $and: [{ averageRating: { $gte: 6 } }, { numVotes: { $gte: 50 } }] }, (err, data) => {
        data.forEach(element => {
            ID.push(element.tconst)
            Votes.push(element.numVotes)
            Ratings.push(element.averageRating)
        });
        k = 0;
        loopArray()
        function loopArray() {
            if (k < ID.length) {
                bringData(() => {
                    k++;
                    loopArray()
                })
            } else {
                res.send({ 'status': 1, 'data': finalData })
            }
        }
        async function bringData(callback) {
            var obj = {}
            var titlearr = await Title.find({ $and: [{ tconst: ID[k] }, { startYear: { $gte: "2015" } }] })
            console.log(titlearr, ID[k]);
            if (titlearr.length > 0) {
                obj.rating = Ratings[k]
                obj.votes = Votes[k]
                obj.titleID = ID[k]
                obj.year = titlearr[0].startYear
                console.log("obj", obj);
                finalData.push(obj);
                Analysis.create(obj, (err, data) => {
                    callback()
                })
            } else {
                callback()
            }

            // Title.find({ tconst: ID[k] }, (err, title) => {
            //     // Title.find({ $and: [{ tconst: ID[k] }, { startYear: { $gte: "2015" } }] }, (err, title) => {
            //     console.log("title", title);
            //     if (title.length > 0) {
            //         obj.rating = Ratings[k]
            //         obj.votes = Votes[k]
            //         obj.titleID = ID[k]
            //         obj.year = title[0].startYear
            //         finalData.push(obj);
            //     }
            //     callback()
            // })
        }
    })
    // averageRating, numVotes
})

router.post("/organalyze", (req, res) => {
    var titleID = []
    var startYear = []
    var titleType = []
    var names = []
    var finalData = []
    Title.find({ $and: [{ startYear: { $gte: "2015" } }, { $or: [{ "titleType": "tvMiniSeries" }, { "titleType": "tvSeries" }] }] }, (err, title) => {
        if (title.length > 0) {
            title.forEach(element => {
                titleID.push(element.tconst)
                startYear.push(element.startYear)
                titleType.push(element.titleType)
                names.push(element.primaryTitle)
            });
        }
        // { $or: [{ "titleType": "tvMiniSeries" }, { "titleType": "tvSeries" }
        console.log("titleID", titleID.length);
        k = 0;
        loopArray()
        function loopArray() {
            if (k < titleID.length) {
                bringData(() => {
                    k++;
                    loopArray()
                })
            } else {
                res.send({ 'status': 1, 'message': 'Done task' })
            }
        }
        function bringData(callback) {
            var obj = {}
            Rating.findOne({ $and: [{ numVotes: { $gte: "50" } }, { averageRating: { $gte: "6" } }, { tconst: titleID[k] }] }, (err, rat) => {
                if (rat) {
                    obj.rating = rat.averageRating
                    obj.votes = rat.numVotes
                    obj.titleID = titleID[k]
                    obj.year = startYear[k]
                    obj.titleType = titleType[k]
                    obj.name = names[k]
                    Analysis.findOne({ titleID: titleID[k] }, (err, findTitle) => {
                        if (!findTitle) {
                            AKAS.findOne({ titleId: titleID[k] }, (err, akas) => {
                                if (akas) {
                                    if (akas.region != "" || akas.region != "\N") {
                                        obj.region = akas.region
                                    } else {
                                        obj.region = ""
                                    }
                                } else {
                                    obj.region = ""
                                }
                                console.log("obj-add", obj);
                                finalData.push(obj);
                                Analysis.create(obj, (err, data) => {
                                    callback()
                                })
                            })
                        } else {
                            console.log("obj", obj);
                            callback()
                        }
                    })
                } else {
                    console.log("obj", obj);
                    callback()
                }
            })
        }
    })
    // db.titles.find({startYear : {$gte : "2015"}}).count()
})

router.get("/orginalmerge", (req, res) => {
    var date = new Date
    var todays_date = date.getDate() + "-" + Number(date.getMonth() + 1) + "-" + date.getFullYear()
    console.log(todays_date);
    Cron_table.findOne({ operation_date: todays_date }, async (err, cron) => {
        // Cron_table.findOne({ operation_date: '2-2-2021' }, async (err, cron) => {
        if (cron) {
            if ((cron.is_dumping_rating == "1" || cron.is_dumping_rating == 1) && (cron.is_dumping_title == "1" || cron.is_dumping_title == 1) && (cron.is_dumping_akas == "1" || cron.is_dumping_akas == 1)) {
                if (cron.is_merge_done == 1 || cron.is_merge_done == "1") {
                    res.send({ 'status': 1, 'message': 'Merging done successfully for this week.' })
                } else {
                    var margin_of_1 = 0
                    var above_500_votes = 0
                    var above_6 = 0
                    var below_6 = 0
                    var key_value_number = 1
                    var key_value_number_votes = 1
                    var merge_data = []
                    var votes = []
                    var ratings = []
                    var titleID = []

                    // var query1 = db.titles.aggregate([{ $match: { $and: [{ startYear: { $gte: "2015" } }, { startYear: { $ne: "\\N" } }, { $or: [{ "titleType": "tvMiniSeries" }, { "titleType": "tvSeries" }] }] } }, { $lookup: { from: "ratings", localField: "tconst", foreignField: "tconst", as: "titleID" } }, { $lookup: { from: "akas", localField: "tconst", foreignField: "titleId", as: "region" } }, { $project: { startYear: 1, tconst: 1, titleType: 1, averageRating: "$titleID.averageRating", numVotes: "$titleID.numVotes", genres: 1, primaryTitle: 1, region : "$region.region" } }])
                    var query1 = [{ $match: { $and: [{ startYear: { $gte: "2015" } }, { startYear: { $ne: "\\N" } }, { $or: [{ "titleType": "tvMiniSeries" }, { "titleType": "tvSeries" }] }] } }, { $lookup: { from: "ratings", localField: "tconst", foreignField: "tconst", as: "titleID" } }, { $lookup: { from: "akas", localField: "tconst", foreignField: "titleId", as: "region" } }, { $lookup: { from: "akas", localField: "tconst", foreignField: "titleId", as: "language" } }, { $project: { startYear: 1, tconst: 1, titleType: 1, averageRating: "$titleID.averageRating", numVotes: "$titleID.numVotes", genres: 1, primaryTitle: 1, region: "$region.region" } }]

                    var query2 = [
                        {
                            $match: {
                                $and: [{ startYear: { $gte: "2015" } }, { startYear: { $ne: "\\N" } },
                                { $or: [{ "titleType": "tvMiniSeries" }, { "titleType": "tvSeries" }] }]
                            }
                        }, {
                            $lookup: { from: 'ratings', localField: "tconst", foreignField: "tconst", as: "rate" }
                        }, {
                            $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "region" }
                        }, {
                            $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "language" }
                        }, {
                            $project: { startYear: 1, tconst: 1, titleType: 1, averageRating: "$rate.averageRating", numVotes: "$rate.numVotes", genres: 1, region: "$region.region", primaryTitle: 1, language: "$language.language" }
                        },
                        { $match: { $and: [{ averageRating: { $gte: "6" } }] } }
                    ]
                    Title.aggregate(query2, (err, data) => {
                        above_6 = data.length
                        data.forEach(element => {
                            titleID.push(element.tconst)
                            ratings.push(element.averageRating[0])
                            votes.push(element.numVotes[0])
                        });
                        k = 0;
                        loopArray()
                        async function loopArray() {
                            if (k < data.length) {
                                bringData(() => {
                                    k++;
                                    loopArray()
                                })
                            } else {
                                var data_to_insert_in_cron_table = {
                                    margin_of_1,
                                    above_500_votes,
                                    above_6,
                                    below_6,
                                    created_on: new Date()
                                }
                                if (merge_data.length > 0) {
                                    var merge_create = await Merge.insertMany(merge_data)
                                }

                                // ****
                                var date = new Date
                                var todays_date = date.getDate() + "-" + Number(date.getMonth() + 1) + "-" + date.getFullYear()
                                var last_week_date = new Date(new Date().setDate(new Date().getDate() - 7));
                                var last_week = last_week_date.getDate() + "-" + Number(last_week_date.getMonth() + 1) + "-" + last_week_date.getFullYear()

                                Cron_table.findOne({ operation_date: last_week }, (err, cron_data) => {
                                    if (cron_data) {
                                        var below = Number(cron_data.above_6) - Number(above_6)
                                        if (below > 0) {
                                            data_to_insert_in_cron_table.below_6 = below
                                        }
                                    }
                                    data_to_insert_in_cron_table.is_merge_done = 1
                                    data_to_insert_in_cron_table.is_merge_done_datetime = new Date()
                                    Cron_table.updateOne({ operation_date: todays_date }, data_to_insert_in_cron_table, (err, cron_update) => {
                                        // *****send email*****
                                        above_6 = merge.length
                                        below_6 = data[0].count - merge.length
                                        var transporter = nodemailer.createTransport({
                                            host: emailtest.hostname,
                                            port: 587,
                                            secure: false, // true for 465, false for other ports
                                            auth: {
                                                user: emailtest.userName,
                                                pass: emailtest.apiKey
                                            }
                                        });
                                        var mailOptions = "";
                                        link =
                                            "<p>ALERTS REQUIRED BY AQUISITION TEAM</p><br><p>The following are the alerts raised for IMDb data analysis</p><br><ul><li>All Shows that have increased from Above 6 by a margin of 1 full point in ratings : " + margin_of_1 + "</li><li> All Shows that have increased from Above 6 and an increase of atleast 500 votes: " + above_500_votes + "</li><li>All Shows that have decreased their rating from above 6 to below 6 : " + below_6 + "</li><li>All Shows that have increased their rating from below 6 to above 6 : " + above_6 + "</li></ul>";
                                        mailOptions = {
                                            from: emailtest.fromEmail,
                                            to: "harapriya.akella@gmail.com",
                                            subject: "ALERTS EMAIL",
                                            html: link
                                        };
                                        transporter.sendMail(mailOptions, function (error, info) {
                                            if (error) {
                                                res.send({ status: 0, message: error.stack });
                                            } else {
                                                res.send({ status: 1, data: data_to_insert_in_cron_table })
                                            }
                                        });
                                        // ********************
                                        // res.send({ status: 1, data: data_to_insert_in_cron_table })
                                    })
                                })
                                // ****
                            }
                        }
                        async function bringData(callback) {
                            var numVotes = []
                            var averageRating = []

                            Merge.findOne({ tconst: titleID[k] }, async (err, merge) => {
                                console.log("titleID[k]", titleID[k]);
                                if (merge) {
                                    for (var i = 50; i > 0; i--) {
                                        var key = "week" + i + "_rating"
                                        if (merge[key] != null) {
                                            key_value_number = i + 1;
                                            break;
                                        }
                                    }
                                    for (var j = 50; j > 0; j--) {
                                        var key = "week" + j + "_rating"
                                        if (merge[key] != null) {
                                            key_value_number_votes = j + 1;
                                            break;
                                        }
                                    }
                                    var key_rating = "week" + key_value_number + "_rating"
                                    var week_number = "week" + key_value_number
                                    var key_votes = "week" + key_value_number_votes + "_votes"

                                    averageRating.push(ratings[k])
                                    numVotes.push(votes[k])
                                    var d = new Date();
                                    // d.setDate(d.getDate() - 5);
                                    var data_to_update = { [key_rating]: ratings[k], [key_votes]: votes[k], averageRating, numVotes, [week_number]: d }
                                    var update_merge = await Merge.updateOne({ tconst: titleID[k] }, data_to_update)

                                    // ****************
                                    var present = Number(ratings[k])
                                    var past = merge["week" + Number(key_value_number - 1) + "_rating"]

                                    if (Number(present - past) == 1 || Number(present - past) == "1") {
                                        margin_of_1++;
                                    }
                                    // var present_votes = merge["week" + Number(key_value_number_votes) + "_votes"]
                                    var present_votes = Number(votes[k])
                                    var past_votes = merge["week" + Number(key_value_number_votes - 1) + "_votes"]
                                    console.log("here", k);
                                    if (Number(present_votes - past_votes) == 1 || Number(present_votes - past_votes) == "1") {
                                        above_500_votes++;
                                    }
                                    // check this in the morning
                                    // "margin_of_1": 0,
                                    // "above_500_votes": 0,
                                    // "above_6": 6886,
                                    // "below_6": 0,
                                    // ****************
                                } else {
                                    data[k].week1_rating = ratings[k]
                                    data[k].week1_votes = votes[k]
                                    var d = new Date();
                                    // d.setDate(d.getDate() - 5);
                                    // data[k].week1 = new Date()
                                    data[k].week1 = d
                                    merge_data.push(data[k])
                                }
                                callback()
                            })
                        }
                    })
                }
            } else {
                res.send({ 'status': 1, 'message': 'Tables not ready for merging' })
            }
        } else {
            res.send({ 'status': 1, 'message': 'Date not matched' })
        }
    })
})
// run for 5 minutes after 5 o clock
router.get("/original-merge", (req, res) => {
    var operationDates = []
    Cron_table.find({}, (err, cronAll) => {
        if (cronAll.length > 0) {
            cronAll.forEach(element => {
                operationDates.push(element.operation_date)
            });
            l = 0
            loopOperationDates()
            function loopOperationDates() {
                if (l < operationDates.length) {
                    bringDataFromCronTable(() => {
                        l++;
                        loopOperationDates()
                    })
                } else {
                    res.send({ 'status': 1, 'message': 'Merging done' })
                }
            }
            function bringDataFromCronTable(callback_cron) {
                Cron_table.findOne({ operation_date: operationDates[l] }, async (err, cron) => {
                    // Cron_table.findOne({ operation_date: '2-2-2021' }, async (err, cron) => {
                    if (cron) {
                        var limit = cron.limit
                        var offset = cron.offset
                        if ((cron.is_dumping_rating == "1" || cron.is_dumping_rating == 1) && (cron.is_dumping_title == "1" || cron.is_dumping_title == 1) && (cron.is_dumping_akas == "1" || cron.is_dumping_akas == 1)) {
                            if (cron.is_merge_done == 1 || cron.is_merge_done == "1") {
                                // res.send({ 'status': 1, 'message': 'Merging done successfully for this week.' })
                                callback_cron()
                            } else {
                                console.log("MERGING STARTED");
                                var margin_of_1 = 0
                                var above_500_votes = 0
                                var above_6 = 0
                                var below_6 = 0
                                var key_value_number = 1
                                var key_value_number_votes = 1
                                var merge_data = []
                                var votes = []
                                var ratings = []
                                var titleID = []
                                // db.titles.aggregate([{$match: {$and: [{ startYear: { $gte: "2015" } }, { startYear: { $ne: "\\N" } },{ $or: [{ "titleType": "tvMiniSeries" }, { "titleType": "tvSeries" }] }]}}, {$lookup: { from: 'ratings', localField: "tconst", foreignField: "tconst", as: "rate" }}, {$lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "region" }}, {$lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "language" }}, {$project: { startYear: 1, tconst: 1, titleType: 1, averageRating: "$rate.averageRating", numVotes: "$rate.numVotes", genres: 1, region: "$region.region", primaryTitle: 1, language: "$language.language" }},{ $match: { $and: [{ averageRating: { $gte: "6" } }] } }, {$skip : 5000}, {$limit: 500}])
                                var query1 = [{ $match: { $and: [{ startYear: { $gte: "2015" } }, { startYear: { $ne: "\\N" } }, { $or: [{ "titleType": "tvMiniSeries" }, { "titleType": "tvSeries" }] }] } }, { $lookup: { from: "ratings", localField: "tconst", foreignField: "tconst", as: "titleID" } }, { $lookup: { from: "akas", localField: "tconst", foreignField: "titleId", as: "region" } }, { $lookup: { from: "akas", localField: "tconst", foreignField: "titleId", as: "language" } }, { $project: { startYear: 1, tconst: 1, titleType: 1, averageRating: "$titleID.averageRating", numVotes: "$titleID.numVotes", genres: 1, primaryTitle: 1, region: "$region.region" } }]

                                var query2 = [
                                    {
                                        $match: {
                                            $and: [{ startYear: { $gte: "2015" } }, { startYear: { $ne: "\\N" } },
                                            { $or: [{ "titleType": "tvMiniSeries" }, { "titleType": "tvSeries" }] }]
                                        }
                                    }, {
                                        $lookup: { from: 'ratings', localField: "tconst", foreignField: "tconst", as: "rate" }
                                    }, {
                                        $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "region" }
                                    }, {
                                        $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "language" }
                                    }, {
                                        $project: { startYear: 1, tconst: 1, titleType: 1, averageRating: "$rate.averageRating", numVotes: "$rate.numVotes", genres: 1, region: "$region.region", primaryTitle: 1, language: "$language.language" }
                                    },
                                    { $match: { $and: [{ averageRating: { $gte: "6" } }] } },
                                    { $skip: offset },
                                    { $limit: limit },
                                ]
                                Title.aggregate(query2, (err, data) => {
                                    console.log("err, data", err, data.length);
                                    // return;
                                    if (data.length > 0) {
                                        // above_6 = data.length
                                        data.forEach(element => {
                                            titleID.push(element.tconst)
                                            ratings.push(element.averageRating[0])
                                            votes.push(element.numVotes[0])
                                        });
                                        k = 0;
                                        loopArray()
                                        async function loopArray() {
                                            if (k < data.length) {
                                                bringData(() => {
                                                    k++;
                                                    loopArray()
                                                })
                                            } else {
                                                var merge_create = await Merge.insertMany(merge_data)
                                                var newOffset = Number(offset) + 500
                                                var update_cron = await Cron_table.updateOne({ operation_date: operationDates[l] }, { offset: newOffset })
                                                // console.log("merge_data", merge_data);
                                                callback_cron()
                                                // ****
                                            }
                                        }
                                        async function bringData(callback) {
                                            var numVotes = []
                                            var averageRating = []

                                            Merge.findOne({ tconst: titleID[k] }, async (err, merge) => {
                                                console.log("titleID[k]", titleID[k]);
                                                if (merge) {
                                                    for (var i = 50; i > 0; i--) {
                                                        var key = "week" + i + "_rating"
                                                        if (merge[key] != null) {
                                                            key_value_number = i + 1;
                                                            break;
                                                        }
                                                    }
                                                    for (var j = 50; j > 0; j--) {
                                                        var key = "week" + j + "_rating"
                                                        if (merge[key] != null) {
                                                            key_value_number_votes = j + 1;
                                                            break;
                                                        }
                                                    }
                                                    var key_rating = "week" + key_value_number + "_rating"
                                                    var week_number = "week" + key_value_number
                                                    var key_votes = "week" + key_value_number_votes + "_votes"

                                                    averageRating.push(ratings[k])
                                                    numVotes.push(votes[k])
                                                    var d = new Date();
                                                    // d = d.setDate(d.getDate() - 0);
                                                    // d.setDate(d.getDate() - 5);
                                                    var data_to_update = { [key_rating]: ratings[k], [key_votes]: votes[k], averageRating, numVotes, [week_number]: d }
                                                    var update_merge = await Merge.updateOne({ tconst: titleID[k] }, data_to_update)
                                                } else {
                                                    data[k].week1_rating = ratings[k]
                                                    data[k].week1_votes = votes[k]
                                                    var d = new Date();
                                                    d = d.setDate(d.getDate() - 0);
                                                    // data[k].week1 = new Date()
                                                    data[k].week1 = d
                                                    merge_data.push(data[k])
                                                }
                                                callback()
                                            })
                                        }
                                    } else {
                                        // compare and send email
                                        if (cron.is_merge_done == 0 || !cron.is_merge_done || cron.is_merge_done == "" || cron.is_merge_done == "0") {
                                            Cron_table.updateOne({ operation_date: operationDates[l] }, { is_merge_done: 1 }, (err, up_cron) => {
                                                callback_cron()
                                            })
                                        } else {
                                            callback_cron()
                                        }
                                    }
                                })
                            }
                        } else {
                            // res.send({ 'status': 1, 'message': 'Tables not ready for merging' })
                            callback_cron()
                        }
                    } else {
                        // res.send({ 'status': 1, 'message': 'Date not matched' })
                        callback_cron()
                    }
                })
            }
        } else {
            res.send({ 'status': 1, 'message': 'Nothing to Merge' })
        }
    })
})

router.post("/week", (req, res) => {
    var key_value_number = 1
    var merge = {
        week1_rating: 1,
        week2_rating: 2,
        // week3_rating: 3,
        // week4_rating: 4,
        // week5_rating: 5,
        // week6_rating: 6,
        // week7_rating: 7,
        // week8_rating: 8,
        // week9_rating: 9,
        // week10_rating: 10,
        // week11_rating: 11,
        // week12_rating: 12,
    }
    for (let i = 12; i > 0; i--) {
        var key = "week" + i + "_rating"
        // console.log("week", key, merge[key]);
        if (merge[key] != null) {
            key_value_number = i + 1;
            console.log("i", i);
            console.log("i++", key_value_number);

            console.log("there", key, merge[key]);
            console.log(i++);
            break;
        }
    }
    console.log("here first");
    var key = "week" + key_value_number + "_rating"
    merge[key] = 2
    console.log(key);
    console.log(merge);
})

router.post("/orgdownload", (req, res) => {
    var url = "https://datasets.imdbws.com/"
    var finalData = []
    var allPaths = []
    var tsvPaths = []
    const request = require('request');
    request(url, function (error, response, body) {
        var data = body.split("<ul>")
        // console.log(data);
        function manipulate(str) {
            return str.split("href").pop().split(">").shift().split("=")[1]
        }
        for (let i = 1; i < 8; i++) {
            finalData.push(manipulate(data[i]))
        }
        k = 0;
        downloadLoop()
        function downloadLoop() {
            if (k < finalData.length) {
                createStream(async (inc) => {
                    console.log("inc", inc);
                    var path = allPaths[k]
                    // k++;
                    // downloadLoop()
                    // setTimeout(function () { unzip(path) }, 15000)
                    // function unzip(path) {
                    //     console.log("path", path)
                    //     var target_path = path.slice(0, -3)
                    //     tsvPaths.push(target_path)
                    //     const fileContents = fs.createReadStream(`${path}`)
                    //     const writeStream = fs.createWriteStream(`${target_path}`)
                    //     // const unzip = zlib.createGunzip()
                    //     //     fileContents.pipe(unzip).pipe(writeStream)
                    //     k++
                    //     downloadLoop()
                    // }
                    var unz = await unzip(path);
                    k++;
                    async function unzip(path) {
                        return new Promise(async resolve => {
                            // console.log("path", path)
                            var target_path = path.slice(0, -3)
                            tsvPaths.push(target_path)
                            const fileContents = fs.createReadStream(`${path}`)
                            const writeStream = fs.createWriteStream(`${target_path}`)
                            const unzip = await zlib.createGunzip()
                            fileContents.pipe(unzip).pipe(writeStream).on('end', () => {
                                // k++
                                downloadLoop()
                                resolve(true)
                            }).on('err', (err) => {
                                console.log('file-err', err)
                                resolve(false)
                            });
                        })
                    }
                })
            } else {
                res.send({ 'status': 1, 'message': 'Done' })
            }
        }
        async function createStream(callback) {
            var fileUrl = finalData[k];
            var output = fileUrl.split("/").pop();
            const download = (url, path, callback) => {
                request.head(url, (err, res, body) => {
                    request(url).pipe(fs.createWriteStream(path))
                })
            }
            const url = fileUrl
            var date = new Date()
            var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
            var insert_path = '/var/www/html/imdb/uploads/' + full_date + '/'

            if (!fs.existsSync(insert_path)) {
                console.log("excel_file not exist");
                shell.mkdir("-p", insert_path);
            }
            var path = insert_path + output
            allPaths.push(path)
            //download(url, path)
            //callback()
            // console.log("start call back");
            // request.head(url, (err, res, body) => {
            // request(url).pipe(fs.createWriteStream(path).on('close', () => {
            //     console.log("stream");
            // }))
            console.log(url);
            var file = fs.createWriteStream(path, "utf8");
            var request = http.get(url).on('response', function (res) {
                console.log('in cb');
                res.on('data', function (chunk) {
                    // console.log("chunk", chunk);
                    file.write(chunk);
                }).on('end', function () {
                    console.log("chunk-end");
                    file.end();
                    callback(true);
                }).on('error', function (err) {
                    // clear timeout
                    console.log(err.message);
                    callback(false);
                });
            });
            // callback()

            // })
            // var download1 = await request(url).pipe(fs.createWriteStream(path))
            // callback()
        }
        return
        // https://www.google.com/search?q=download+unzip+read+a+file+node&oq=download+unzip+read+a+file+node+&aqs=chrome..69i57.18945j0j7&sourceid=chrome&ie=UTF-8
    });
})

router.post("/download", (req, res) => {
    var url = "https://datasets.imdbws.com/"
    var finalData = []
    var allPaths = []
    var tsvPaths = []
    const request = require('request');
    request(url, function (error, response, body) {
        var data = body.split("<ul>")
        // console.log(data);
        function manipulate(str) {
            return str.split("href").pop().split(">").shift().split("=")[1]
        }
        for (let i = 1; i < 8; i++) {
            finalData.push(manipulate(data[i]))
        }
        var fileUrl = finalData[6];
        var output = fileUrl.split("/").pop();
        const download = (url, path, callback) => {
            request.head(url, (err, res, body) => {
                request(url).pipe(fs.createWriteStream(path))
            })
        }
        const url = fileUrl
        var date = new Date()
        var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
        var insert_path = '/var/www/html/imdb/uploads/' + full_date + '/'

        if (!fs.existsSync(insert_path)) {
            console.log("excel_file not exist");
            shell.mkdir("-p", insert_path);
        }
        var path = insert_path + output
        allPaths.push(path)
        download(url, path)
        // var target_path = path.slice(0, -3)
        // tsvPaths.push(target_path)
        // const fileContents = fs.createReadStream(`${path}`)
        // const writeStream = fs.createWriteStream(`${target_path}`)
        // const unzip = zlib.createGunzip()
        // fileContents.pipe(unzip).pipe(writeStream)
        res.send({ 'status': 1, 'message': 'done' })
        return
        k = 0;
        downloadLoop()
        function downloadLoop() {
            if (k < finalData.length) {
                createStream(async (inc) => {
                    console.log("inc", inc);
                    var path = allPaths[k]
                    // k++;
                    // downloadLoop()
                    // setTimeout(function () { unzip(path) }, 15000)
                    // function unzip(path) {
                    //     console.log("path", path)
                    //     var target_path = path.slice(0, -3)
                    //     tsvPaths.push(target_path)
                    //     const fileContents = fs.createReadStream(`${path}`)
                    //     const writeStream = fs.createWriteStream(`${target_path}`)
                    //     // const unzip = zlib.createGunzip()
                    //     //     fileContents.pipe(unzip).pipe(writeStream)
                    //     k++
                    //     downloadLoop()
                    // }
                    var unz = await unzip(path);
                    k++;
                    async function unzip(path) {
                        return new Promise(async resolve => {
                            // console.log("path", path)
                            var target_path = path.slice(0, -3)
                            tsvPaths.push(target_path)
                            const fileContents = fs.createReadStream(`${path}`)
                            const writeStream = fs.createWriteStream(`${target_path}`)
                            const unzip = await zlib.createGunzip()
                            fileContents.pipe(unzip).pipe(writeStream).on('end', () => {
                                // k++
                                downloadLoop()
                                resolve(true)
                            }).on('err', (err) => {
                                console.log('file-err', err)
                                resolve(false)
                            });
                        })
                    }
                })
            } else {
                res.send({ 'status': 1, 'message': 'Done' })
            }
        }
        async function createStream(callback) {
            var fileUrl = finalData[k];
            var output = fileUrl.split("/").pop();
            const download = (url, path, callback) => {
                request.head(url, (err, res, body) => {
                    request(url).pipe(fs.createWriteStream(path))
                })
            }
            const url = fileUrl
            var date = new Date()
            var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
            var insert_path = '/var/www/html/imdb/uploads/' + full_date + '/'

            if (!fs.existsSync(insert_path)) {
                console.log("excel_file not exist");
                shell.mkdir("-p", insert_path);
            }
            var path = insert_path + output
            allPaths.push(path)
            //download(url, path)
            //callback()
            // console.log("start call back");
            // request.head(url, (err, res, body) => {
            // request(url).pipe(fs.createWriteStream(path).on('close', () => {
            //     console.log("stream");
            // }))
            console.log(url);
            var file = fs.createWriteStream(path, "utf8");
            var request = http.get(url).on('response', function (res) {
                console.log('in cb');
                res.on('data', function (chunk) {
                    // console.log("chunk", chunk);
                    file.write(chunk);
                }).on('end', function () {
                    console.log("chunk-end");
                    file.end();
                    callback(true);
                }).on('error', function (err) {
                    // clear timeout
                    console.log(err.message);
                    callback(false);
                });
            });
            // callback()

            // })
            // var download1 = await request(url).pipe(fs.createWriteStream(path))
            // callback()
        }
        return
        // https://www.google.com/search?q=download+unzip+read+a+file+node&oq=download+unzip+read+a+file+node+&aqs=chrome..69i57.18945j0j7&sourceid=chrome&ie=UTF-8
    });
})

router.post("/analyze", (req, res) => {

    var emailtest = {
        "fromEmail": "noreply@flickquickapp.com",
        "fromName": "FLICKQUICK",
        "hostname": "smtp.sparkpostmail.com",
        "userName": "SMTP_Injection",
        "apiKey": "bd99cb8ff7487d7c3e89befe064ee3a6e3a6d443"
    }
    const nodemailer = require("nodemailer");
    var margin_of_1 = 0
    var above_500_votes = 0
    var above_6 = 0
    var below_6 = 0
    var titleIDs = []
    var key_value_number = 1
    var key_value_number_votes = 1
    Merge.find({}, (err, merge) => {
        k = 0;
        merge.forEach(element => {
            titleIDs.push(element.tconst)
        });
        loopArray()
        function loopArray() {
            if (k < titleIDs.length) {
                bringData(() => {
                    k++;
                    loopArray()
                })
            } else {
                above_6 = merge.length
                below_6 = data[0].count - merge.length
                var transporter = nodemailer.createTransport({
                    host: emailtest.hostname,
                    port: 587,
                    secure: false, // true for 465, false for other ports
                    auth: {
                        user: emailtest.userName,
                        pass: emailtest.apiKey
                    }
                });
                var mailOptions = "";
                link =
                    "<p>ALERTS REQUIRED BY AQUISITION TEAM</p><br><p>The following are the alerts raised for IMDb data analysis</p><br><ul><li>All Shows that have increased from Above 6 by a margin of 1 full point in ratings : " + margin_of_1 + "</li><li> All Shows that have increased from Above 6 and an increase of atleast 500 votes: " + above_500_votes + "</li><li>All Shows that have decreased their rating from above 6 to below 6 : " + below_6 + "</li><li>All Shows that have increased their rating from below 6 to above 6 : " + above_6 + "</li></ul>";
                mailOptions = {
                    from: emailtest.fromEmail,
                    to: "harapriya.akella@gmail.com",
                    subject: "ALERTS EMAIL",
                    html: link
                };
                transporter.sendMail(mailOptions, function (error, info) {
                    if (error) {
                        res.send({ status: 0, message: error.stack });
                    } else {
                        Cron_table.create({ margin_of_1, above_500_votes, above_6, below_6 }, (err, cron) => {
                            res.send({ 'status': 1, 'message': 'Done', "data": { margin_of_1, above_500_votes, above_6, below_6 } })
                        })
                    }
                });
            }
        }
        function bringData(callback) {
            console.log("k", k);
            Merge.findOne({ tconst: titleIDs[k] }, (err, merge_single) => {
                if (merge_single) {
                    for (var i = 12; i > 0; i--) {
                        var key = "week" + i + "_rating"
                        if (merge_single[key] != null) {
                            key_value_number = i;
                            break;
                        }
                    }
                    for (var j = 12; j > 0; j--) {
                        var key = "week" + j + "_votes"
                        if (merge_single[key] != null) {
                            key_value_number_votes = j;
                            break;
                        }
                    }
                    var present = merge_single["week" + key_value_number + "_rating"]
                    var past = merge_single["week" + Number(key_value_number - 1) + "_rating"]

                    var present_votes = merge_single["week" + key_value_number_votes + "_votes"]
                    var past_votes = merge_single["week" + Number(key_value_number_votes - 1) + "_votes"]
                    if (Number(present - past) == 1 || Number(present - past) == "1") {
                        margin_of_1++;
                    }
                    if (Number(present_votes - past_votes) <= 500) {
                        above_500_votes++;
                    }
                }
                callback()
            })
        }
    })
})

router.post("/test", (req, res) => {
    var arr1 = [
        "601bb0ea8e8e050e143f97d3",
        "602230ea93ef5717c49913c3",
        "601bb0ea8e8e050e143f97d3",
        "602230ea93ef5717c49913c3",
        "601bb0ea8e8e050e143f97d3",
        "602230ea93ef5717c49913c3",
        "601bb0ea8e8e050e143f97d3",
        "602230ea93ef5717c49913c3",
        "601bb0ea8e8e050e143f97d3",
        "602230ea93ef5717c49913c3"
    ]
    var arr2 = ["601bb0ea8e8e050e143f97d3", "602230ea93ef5717c49913c3"]
    var concat = arr1.concat(arr2)
    var new_arr = Array.from(new Set(concat))
    res.send({ new_arr })
    return
    var arr = [
        '/var/www/html/imdb/uploads/29_1_2021/name.basics.tsv',
        '/var/www/html/imdb/uploads/29_1_2021/title.akas.tsv',
        '/var/www/html/imdb/uploads/29_1_2021/title.basics.tsv',
        '/var/www/html/imdb/uploads/29_1_2021/title.crew.tsv',
        '/var/www/html/imdb/uploads/29_1_2021/title.episode.tsv',
        '/var/www/html/imdb/uploads/29_1_2021/title.principals.tsv',
        '/var/www/html/imdb/uploads/29_1_2021/title.ratings.tsv'
    ]
    l = 0;
    tsvLoop()
    function tsvLoop() {
        if (l < arr.length) {
            readTSV(() => {
                l++;
                tsvLoop()
            })
        } else {
            res.send({ 'message': 'Done' })
        }
    }
    function readTSV(callback) {
        var path = arr[l]
        var single = path.split("/").pop()
        if (single == "name.basics.tsv") {
            fs.createReadStream('/var/www/html/imdb/uploads/29_1_2021/name.basics.tsv', "utf8")
                .on("data", async (data) => {
                    console.log("data", data);
                })
        }
        callback()
        return
        switch (single) {
            case "name.basics.tsv":
                console.log("name.basics.tsv");
                // ****************
                var obj = []
                // var readFile = fs.createReadStream('/var/www/html/imdb/uploads/29_1_2021/name.basics.tsv', "utf8")
                // console.log("readFile", readFile);
                fs.createReadStream('/var/www/html/imdb/uploads/29_1_2021/name.basics.tsv', "utf8")
                    // .pipe()
                    // readFile.pipe(res)
                    // return
                    // readFile.on("error", err => console.log(err))
                    .on('data', async (data) => {
                        final_data = d3.tsvParseRows(data)
                        console.log(final_data);
                        obj = final_data.map(function (params) {
                            return {
                                nconst: params[0] == "" || params[0] == "\\N" ? "" : params[0],
                                primary_name: params[1] == "" || params[1] == "\\N" ? "" : params[1],
                                birth_year: params[2] == "" || params[2] == "\\N" ? "" : params[2],
                                death_year: params[3] == "" || params[3] == "\\N" ? "" : params[3],
                                primary_profession: params[4] == "" || params[4] == "\\N" ? "" : params[4],
                                known_for_title: params[5] == "" || params[5] == "\\N" ? "" : params[5],
                            }
                        })
                        k = 0;
                        loopNameArray()
                        function loopNameArray() {
                            if (k < obj.length) {
                                insertNameData(() => {
                                    k++;
                                    loopNameArray()
                                })
                            } else {
                                return
                            }
                        }
                        function insertNameData(callback_internal) {
                            Name_IMDb.findOne({ nconst: obj[k].nconst }, (err, findName) => {
                                if (!findName || !findName.nconst || findName.nconst == "") {
                                    Name_IMDb.create(obj[k], (err, data) => {
                                        callback_internal()
                                    })
                                } else {
                                    callback_internal()
                                }
                            })
                        }
                    })
                    .on('end', async () => {
                        console.log("end");
                        return;
                    })
                break;
            // ****************
            case "title.akas.tsv":
                console.log("title.akas.tsv");
                break;
            case "title.basics.tsv":
                console.log("title.basics.tsv");
                break;
            case "title.crew.tsv":
                console.log("title.crew.tsv");
                break;
            case "title.episode.tsv":
                console.log("title.episode.tsv");
                break;
            case "title.principals.tsv":
                console.log("title.principals.tsv");
                break;
            case "title.ratings.tsv":
                console.log("title.ratings.tsv");
                break;
            default:
            // code block
        }
        callback()
    }
})
// run for 5 minutes after 12 o clock
router.get("/original-download-akas", (req, res) => {
    var date = new Date
    var todays_date = date.getDate() + "-" + Number(date.getMonth() + 1) + "-" + date.getFullYear()
    console.log(todays_date);
    Cron_table.find({ operation_date: todays_date }, async (err, cron) => {
        if (cron.length <= 0) {
            var create_cron = await Cron_table.create({ operation_date: todays_date, limit: 500, offset: 0 })
            var remove_akas = await AKAS.deleteMany({})
            download_upzip()
            res.send({ 'status': 1, 'message': 'Done' })
        } else {
            if (cron[0].is_download_unzip_akas == 1 || cron[0].is_download_unzip_akas == "1") {
                // dump akas
                if (cron[0].is_dumping_akas == 1 || cron[0].is_dumping_akas == "1") {
                    res.send({ 'status': 1, message: 'AKAS dumping done' })
                } else {
                    var akas_count = await AKAS.countDocuments({})
                    if (akas_count > 0) {
                        res.send({ 'status': 1, message: 'AKAS dumping in progress' })
                    } else {
                        var obj = []
                        var dataToAdd = []
                        var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
                        var insert_path = filepath + full_date + '/' + "title.akas.tsv"
                        fs.createReadStream(`${insert_path}`, "utf8")
                            // .pipe()
                            .on('data', async (data) => {
                                var single = {}
                                final_data = d3.tsvParseRows(data)
                                obj = final_data.map(function (params) {
                                    single = {
                                        titleId: params[0] == "" || params[0] == "\\N" ? "" : params[0],
                                        ordering: params[1] == "" || params[1] == "\\N" ? "" : params[1],
                                        title: params[2] == "" || params[2] == "\\N" ? "" : params[2],
                                        region: params[3] == "" || params[3] == "\\N" ? "" : params[3],
                                        language: params[4] == "" || params[4] == "\\N" ? "" : params[4],
                                        types: params[5] == "" || params[5] == "\\N" ? "" : params[5],
                                        attributes: params[6] == "" || params[6] == "\\N" ? "" : params[6],
                                        isOriginalTitle: params[7] == "" || params[7] == "\\N" ? "" : params[7]
                                    }
                                    return single
                                })
                                k = 0;
                                loopAKASArray()
                                function loopAKASArray() {
                                    if (k < obj.length) {
                                        insertAKASData(() => {
                                            k++;
                                            loopAKASArray()
                                        })
                                    } else {
                                        return
                                    }
                                }
                                function insertAKASData(callback_internal) {
                                    if (obj[k].titleId != "" || !obj[k].titleId) {
                                        var word = obj[k].titleId
                                        if (word.startsWith("tt")) {
                                            AKAS.insertMany(obj[k], (err, data) => {
                                                callback_internal()
                                            })
                                        }
                                    }
                                    callback_internal()
                                }
                            })
                            .on('end', async () => {
                                Cron_table.updateOne({ operation_date: todays_date }, { is_dumping_akas: 1 }, (err, update_akas) => {
                                    res.send({ message: "Task done", data: dataToAdd.length })
                                })
                                // return
                            })
                    }
                }
            } else {
                download_upzip()
                res.send({ 'status': 1, 'message': 'Done' })
            }
        }
    })
    function download_upzip() {
        var url = "https://datasets.imdbws.com/"
        var finalData = []
        var allPaths = []
        var tsvPaths = []
        const request = require('request');
        request(url, function (error, response, body) {
            var data = body.split("<ul>")
            // console.log(data);
            function manipulate(str) {
                return str.split("href").pop().split(">").shift().split("=")[1]
            }
            for (let i = 1; i < 8; i++) {
                finalData.push(manipulate(data[i]))
            }
            var fileUrl = finalData[1];
            var output = fileUrl.split("/").pop();
            const url = fileUrl
            var date = new Date()
            var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
            var insert_path = filepath + full_date + '/'

            if (!fs.existsSync(insert_path)) {
                console.log("excel_file not exist");
                shell.mkdir("-p", insert_path);
            }
            var path = insert_path + output
            var file = fs.createWriteStream(path, "utf8");
            var request = http.get(url).on('response', function (res) {
                console.log('in cb');
                res.on('data', function (chunk) {
                    file.write(chunk);
                    console.log(chunk);
                }).on('end', function () {
                    console.log("chunk-end");
                    var target_path = path.slice(0, -3)
                    const fileContents = fs.createReadStream(`${path}`)
                    const writeStream = fs.createWriteStream(`${target_path}`)
                    const unzip = zlib.createGunzip()
                    fileContents.pipe(unzip).pipe(writeStream)
                    console.log("unzip-ended");
                    Cron_table.updateOne({ operation_date: todays_date }, { is_download_unzip_akas: 1, is_download_unzip_akas_datetime: new Date }, (err, update_cron) => {
                        // file.end();
                        // res.send({ 'status': 1, 'message': 'Done' })
                        return
                    })
                }).on('error', function (err) {
                    // clear timeout
                    console.log(err.message);
                });
            });
        })
    }
})
// run for 30 minutes after 12 o clock
router.get("/original-download-title", (req, res) => {
    var date = new Date
    var todays_date = date.getDate() + "-" + Number(date.getMonth() + 1) + "-" + date.getFullYear()
    console.log(todays_date);
    Cron_table.find({ operation_date: todays_date }, async (err, cron) => {
        if (cron.length <= 0) {
            var create_cron = await Cron_table.create({ operation_date: todays_date, limit: 500, offset: 0 })
            var remove_title = await Title.deleteMany({})
            download_upzip()
            res.send({ 'status': 1, 'message': 'Done' })
        } else {
            if (cron[0].is_download_unzip_title == 1 || cron[0].is_download_unzip_title == "1") {
                // dump title
                if (cron[0].is_dumping_title == 1 || cron[0].is_dumping_title == "1") {
                    res.send({ 'status': 1, message: 'Title dumping done' })
                } else {
                    var title_count = await Title.countDocuments({})
                    if (title_count > 0) {
                        res.send({ 'status': 1, message: 'Titles dumping in progress' })
                    } else {
                        var obj = []
                        var final_data = []
                        var dataToAdd = []
                        var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
                        var insert_path = filepath + full_date + '/' + "title.basics.tsv"
                        fs.createReadStream(`${insert_path}`, "utf8")
                            // .pipe()
                            .on('data', (data) => {
                                final_data = d3.tsvParseRows(data)
                                obj = final_data.map(function (params) {
                                    return {
                                        tconst: !params[0] || params[0] == "" || params[0] == "\N" ? "" : params[0],
                                        titleType: !params[1] || params[1] == "" || params[1] == "\N" ? "" : params[1],
                                        primaryTitle: !params[2] || params[2] == "" || params[2] == "\N" ? "" : params[2],
                                        originalTitle: !params[3] || params[3] == "" || params[3] == "\N" ? "" : params[3],
                                        isAdult: !params[4] || params[4] == "" || params[4] == "\N" ? "" : params[4],
                                        startYear: !params[5] || params[5] == "" || params[5] == "\N" ? "" : params[5],
                                        endYear: !params[6] || params[6] == "" || params[6] == "\N" ? "" : params[6],
                                        runtimeMinutes: !params[7] || params[7] == "" || params[7] == "\N" ? "" : params[7],
                                        genres: !params[8] || params[8] == "" || params[8] == "\N" ? "" : params[8]
                                    }
                                })
                                k = 0;
                                loopArray()
                                function loopArray() {
                                    if (k < obj.length) {
                                        insertData(() => {
                                            k++;
                                            loopArray()
                                        })
                                    } else {
                                        return;
                                    }
                                }
                                function insertData(callback) {

                                    if (obj[k].tconst != "" || !obj[k].tconst) {
                                        var word = obj[k].tconst
                                        if (word.startsWith("tt")) {
                                            Title.insertMany(obj[k], (err, data) => {
                                                callback()
                                            })
                                        }
                                    }
                                    callback()
                                }
                            })
                            .on('end', async () => {
                                Cron_table.updateOne({ operation_date: todays_date }, { is_dumping_title: 1 }, (err, update_title) => {
                                    res.send({ message: "Task done" })
                                })
                            })
                    }
                }
            } else {
                download_upzip()
                res.send({ 'status': 1, 'message': 'Done' })
            }
        }
    })
    function download_upzip() {
        var url = "https://datasets.imdbws.com/"
        var finalData = []
        var allPaths = []
        var tsvPaths = []
        const request = require('request');
        request(url, function (error, response, body) {
            var data = body.split("<ul>")
            // console.log(data);
            function manipulate(str) {
                return str.split("href").pop().split(">").shift().split("=")[1]
            }
            for (let i = 1; i < 8; i++) {
                finalData.push(manipulate(data[i]))
            }
            var fileUrl = finalData[2];
            var output = fileUrl.split("/").pop();
            const url = fileUrl
            var date = new Date()
            var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
            var insert_path = filepath + full_date + '/'

            if (!fs.existsSync(insert_path)) {
                console.log("excel_file not exist");
                shell.mkdir("-p", insert_path);
            }
            var path = insert_path + output
            var file = fs.createWriteStream(path, "utf8");
            var request = http.get(url).on('response', function (res) {
                console.log('in cb');
                res.on('data', function (chunk) {
                    file.write(chunk);
                    console.log(chunk);
                }).on('end', function () {
                    console.log("chunk-end");
                    var target_path = path.slice(0, -3)
                    const fileContents = fs.createReadStream(`${path}`)
                    const writeStream = fs.createWriteStream(`${target_path}`)
                    const unzip = zlib.createGunzip()
                    fileContents.pipe(unzip).pipe(writeStream)
                    console.log("unzip-ended");
                    Cron_table.updateOne({ operation_date: todays_date }, { is_download_unzip_title: 1, is_download_unzip_title_datetime: new Date }, (err, update_cron) => {
                        // file.end();
                        // res.send({ 'status': 1, 'message': 'Done' })
                        return
                    })
                }).on('error', function (err) {
                    // clear timeout
                    console.log(err.message);
                });
            });
        })
    }
})
// run for 2 hour after 12 o clock
router.get("/original-download-rating", (req, res) => {
    var date = new Date
    var todays_date = date.getDate() + "-" + Number(date.getMonth() + 1) + "-" + date.getFullYear()
    console.log(todays_date);
    Cron_table.find({ operation_date: todays_date }, async (err, cron) => {
        if (cron.length <= 0) {
            var create_cron = await Cron_table.create({ operation_date: todays_date, limit: 500, offset: 0 })
            var remove_rating = await Rating.deleteMany({})
            download_upzip()
            res.send({ 'status': 1, 'message': 'Done' })
        } else {
            if (cron[0].is_download_unzip_rating == 1 || cron[0].is_download_unzip_rating == "1") {
                // dump rating
                if (cron[0].is_dumping_rating == 1 || cron[0].is_dumping_rating == "1") {
                    res.send({ 'status': 1, message: 'Ratings dumping done' })
                } else {
                    var rating_count = await Rating.countDocuments({})
                    if (rating_count > 0) {
                        res.send({ 'status': 1, message: 'Rating dumping in progress' })
                    } else {
                        var obj = []
                        var final_data = []
                        var dataToAdd = []
                        var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
                        var insert_path = filepath + full_date + '/' + "title.ratings.tsv"
                        fs.createReadStream(`${insert_path}`, "utf8")
                            .on('data', async (data) => {
                                final_data = d3.tsvParseRows(data)
                                obj = final_data.map(function (params) {
                                    return {
                                        tconst: params[0] == "" || params[0] == "\\N" ? "" : params[0],
                                        averageRating: params[1] == "" || params[1] == "\\N" ? "" : params[1],
                                        numVotes: params[2] == "" || params[2] == "\\N" ? "" : params[2],
                                    }
                                })
                                k = 0;
                                loopRatingArray()
                                function loopRatingArray() {
                                    if (k < obj.length) {
                                        insertRatingData(() => {
                                            k++;
                                            loopRatingArray()
                                        })
                                    } else {
                                        return;
                                    }
                                }
                                function insertRatingData(callback_internal) {
                                    if (obj[k].tconst != "" || !obj[k].tconst) {
                                        var word = obj[k].tconst
                                        if (word.startsWith("tt")) {
                                            Rating.insertMany(obj[k], (err, data) => {
                                                callback_internal()
                                            })
                                        }
                                    }
                                    callback_internal()
                                }
                            })
                            .on('end', async () => {
                                Cron_table.updateOne({ operation_date: todays_date }, { is_dumping_rating: 1 }, (err, update_rating) => {
                                    res.send({ message: "Task done" })
                                })
                            })
                    }
                }
            } else {
                download_upzip()
                res.send({ 'status': 1, 'message': 'Done' })
            }
        }
    })
    function download_upzip() {
        var url = "https://datasets.imdbws.com/"
        var finalData = []
        var allPaths = []
        var tsvPaths = []
        const request = require('request');
        request(url, function (error, response, body) {
            var data = body.split("<ul>")
            // console.log(data);
            function manipulate(str) {
                return str.split("href").pop().split(">").shift().split("=")[1]
            }
            for (let i = 1; i < 8; i++) {
                finalData.push(manipulate(data[i]))
            }
            var fileUrl = finalData[6];
            var output = fileUrl.split("/").pop();
            const url = fileUrl
            var date = new Date()
            var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
            var insert_path = filepath + full_date + '/'

            if (!fs.existsSync(insert_path)) {
                console.log("excel_file not exist");
                shell.mkdir("-p", insert_path);
            }
            var path = insert_path + output
            var file = fs.createWriteStream(path, "utf8");
            var request = http.get(url).on('response', function (res) {
                console.log('in cb');
                res.on('data', function (chunk) {
                    file.write(chunk);
                    console.log(chunk);
                }).on('end', function () {
                    console.log("chunk-end");
                    var target_path = path.slice(0, -3)
                    const fileContents = fs.createReadStream(`${path}`)
                    const writeStream = fs.createWriteStream(`${target_path}`)
                    const unzip = zlib.createGunzip()
                    fileContents.pipe(unzip).pipe(writeStream)
                    console.log("unzip-ended");
                    Cron_table.updateOne({ operation_date: todays_date }, { is_download_unzip_rating: 1, is_download_unzip_rating_datetime: new Date }, (err, update_cron) => {
                        file.end();
                        return
                    })
                }).on('error', function (err) {
                    // clear timeout
                    console.log(err.message);
                });
            });
        })
    }
})
// run for 1 hour after 12 o clock
router.get("/difference", async (req, res) => {
    var cronTables = await Cron_table.find({})
    if (cronTables[1].is_mail_sent == 1 || cronTables[1].is_mail_sent == '1') {
        res.send({ 'status': 1, 'message': 'Difference calculation successfully completed' })
    } else {
        var emailtest = {
            "fromEmail": "noreply@flickquickapp.com",
            "fromName": "FLICKQUICK",
            "hostname": "smtp.sparkpostmail.com",
            "userName": "SMTP_Injection",
            "apiKey": "bd99cb8ff7487d7c3e89befe064ee3a6e3a6d443"
        }
        const nodemailer = require("nodemailer");

        var titleIDs = []
        var ratings = []
        var votes = []
        var margin_of_1 = 0
        var above_500_votes = 0
        var above_6 = 0
        var below_6 = 0
        var key_value_number = 1
        var key_value_number_votes = 1
        // created_on: new Date()
        var allMerges = await Merge.find({})
        allMerges.forEach(element => {
            titleIDs.push(element.tconst)
            votes.push(element.numVotes)
            ratings.push(element.averageRating)
        });

        k = 0;
        loopArray()
        async function loopArray() {
            if (k < titleIDs.length) {
                bringData(() => {
                    k++;
                    loopArray()
                })
            } else {
                var data_to_insert_in_cron_table = {
                    margin_of_1,
                    above_500_votes,
                    above_6: allMerges.length,
                    created_on: new Date()
                }

                var cronTables = await Cron_table.find({})
                if (cronTables.length >= 2) {
                    data_to_insert_in_cron_table.below_6 = Math.abs(allMerges.length - cronTables[0].above_6)
                } else {
                    data_to_insert_in_cron_table.below_6 = 0
                }
                console.log("data_to_insert_in_cron_table", data_to_insert_in_cron_table);
                Cron_table.updateOne({ _id: cronTables[1]._id }, data_to_insert_in_cron_table, (err, cron_table) => {
                    // **************send email***************
                    var transporter = nodemailer.createTransport({
                        host: emailtest.hostname,
                        port: 587,
                        secure: false, // true for 465, false for other ports
                        auth: {
                            user: emailtest.userName,
                            pass: emailtest.apiKey
                        }
                    });
                    var mailOptions = "";
                    link =
                        "<p>ALERTS REQUIRED BY AQUISITION TEAM</p><br><p>The following are the alerts raised for IMDb data analysis</p><br><ul><li><a href = 'http://139.59.18.134:5000/emaildata?id=1'>All Shows that have increased from Above 6 by a margin of 1 full point in ratings : " + data_to_insert_in_cron_table.margin_of_1 + "</a></li><li><a href = 'http://139.59.18.134:5000/emaildata?id=2'> All Shows that have increased from Above 6 and an increase of atleast 500 votes: " + data_to_insert_in_cron_table.above_500_votes + "</a></li><li><a href = 'http://139.59.18.134:5000/emaildata?id=3'>All Shows that have decreased their rating from above 6 to below 6 : " + data_to_insert_in_cron_table.below_6 + "</a></li><li><a href = 'http://139.59.18.134:5000/emaildata?id=4'>All Shows that have increased their rating from below 6 to above 6 : " + data_to_insert_in_cron_table.above_6 + "</a></li></ul>";
                    mailOptions = {
                        from: emailtest.fromEmail,
                        to: "harapriya.akella@gmail.com",
                        subject: "ALERTS EMAIL",
                        html: link
                    };
                    transporter.sendMail(mailOptions, function (error, info) {
                        console.log("error, info", error, info);
                        if (info) {
                            Cron_table.updateOne({ _id: cronTables[1]._id }, { is_mail_sent: 1 }, (err, sent) => {
                                res.send({ error, info })
                            })
                        }
                    });
                    // ***************************************
                })
            }
        }
        function bringData(callback) {
            Merge.findOne({ tconst: titleIDs[k] }, (err, merge) => {
                console.log(titleIDs[k], k, titleIDs.length);
                // console.log("err,merge", err, merge);
                // ****************************
                for (var i = 50; i > 0; i--) {
                    var key = "week" + i + "_rating"
                    if (merge[key] != null) {
                        key_value_number = i + 1;
                        break;
                    }
                }
                for (var j = 50; j > 0; j--) {
                    var key = "week" + j + "_votes"
                    if (merge[key] != null) {
                        key_value_number_votes = j + 1;
                        break;
                    }
                }
                var key_rating = "week" + key_value_number + "_rating"
                var week_number = "week" + key_value_number
                var key_votes = "week" + key_value_number_votes + "_votes"

                console.log("key_rating", key_rating);
                console.log("week_number", week_number);
                console.log("key_votes", key_votes);

                var present = Number(merge.averageRating)
                var past_key = "week" + Number(key_value_number - 1) + "_rating"
                if (past_key == "week0rating") {
                    // callback()
                    console.log("ZERO");
                } else {
                    var past = merge[past_key]
                    console.log("PAST", past);
                    if (Number(present - past) > 1) {
                        margin_of_1++;
                    }
                    var present_votes = Number(merge.numVotes)
                    var past_votes = merge["week" + Number(key_value_number_votes - 1) + "_votes"]
                    // console.log("here", k);
                    if (Number(present_votes - past_votes) > 500) {
                        above_500_votes++;
                    }
                }
                callback()
                // res.send({ 'message': 'Done', data: { above_500_votes, margin_of_1 } })
                // ****************************
            })
        }
    }
})

router.get("/diff", (req, res) => {
    var margin_of_1 = 0
    var above_500_votes = 0
    var above_6 = 0
    var below_6 = 0
    Merge.findOne({ tconst: 'tt0303570' }, (err, merge) => {
        console.log("err,merge", err, merge);
        // ****************************
        for (var i = 50; i > 0; i--) {
            var key = "week" + i + "_rating"
            if (merge[key] != null) {
                key_value_number = i + 1;
                break;
            }
        }
        res.send({ status: 1, key_value_number, merge })
        return
        for (var j = 50; j > 0; j--) {
            var key = "week" + j + "_rating"
            if (merge[key] != null) {
                key_value_number_votes = j + 1;
                break;
            }
        }
        var key_rating = "week" + key_value_number + "_rating"
        var week_number = "week" + key_value_number
        var key_votes = "week" + key_value_number_votes + "_votes"

        console.log("key_rating", key_rating);
        console.log("week_number", week_number);
        console.log("key_votes", key_votes);

        var present = Number(merge.averageRating)
        var past_key = "week" + Number(key_value_number - 1) + "_rating"
        if (past_key == "week0rating") {
            // callback()
            console.log("ZERO");
        } else {
            var past = merge[past_key]
            console.log("PAST", past);
            if (Number(present - past) > 1) {
                margin_of_1++;
            }
            var present_votes = Number(merge.numVotes)
            var past_votes = merge["week" + Number(key_value_number_votes - 1) + "_votes"]
            // console.log("here", k);
            if (Number(present_votes - past_votes) > 500) {
                above_500_votes++;
            }
        }
        res.send({ 'message': 'Done', data: { above_500_votes, margin_of_1 } })
        // ****************************
    })
})

router.get("/original-margin_of_1", async (req, res) => {
    var titleIDs = []
    var ratings = []
    var key_value_number = 1
    var finalData = []
    var allMerges = await Merge.find({})
    allMerges.forEach(element => {
        titleIDs.push(element.tconst)
        ratings.push(element.averageRating)
    });

    k = 0;
    loopArray()
    async function loopArray() {
        if (k < titleIDs.length) {
            bringData(() => {
                k++;
                loopArray()
            })
        } else {
            res.send({ 'status': 1, 'data': finalData })
        }
    }
    function bringData(callback) {
        Merge.findOne({ tconst: titleIDs[k] }, (err, merge) => {
            console.log(titleIDs[k], k, titleIDs.length);
            for (var i = 50; i > 0; i--) {
                var key = "week" + i + "_rating"
                if (merge[key] != null) {
                    key_value_number = i - 1; // past
                    break;
                }
            }

            var present = Number(merge.averageRating)
            var past_key = "week" + Number(key_value_number) + "_rating"
            var past = merge[past_key]
            console.log("PAST", past);
            if (Number(present - past) > 1) {
                finalData.push(merge)
            }
            callback()
        })
    }
})

router.post("/original-above_6", async (req, res) => {
    var allMerges = []
    var tConstArr = []
    console.log("REQ", req.body);
    if (req.body.limit.toString() && req.body.offset.toString()) {
        var limit = Number(req.body.limit)
        var offset = Number(req.body.offset)
        // ********
        var find_query = { $or: [{ averageRating: { $gte: "6" } }, { averageRating: { $gte: 6 } }] }
        var count = await Merge.countDocuments(find_query)
        var data = await Merge.find(find_query, { __v: 0, _id: 0 }).skip(offset).limit(limit);
        var data = await Merge.aggregate([{ $group: { _id: "$tconst" } }])
        db.test.aggregate([{ $group: { _id: "$name", salary: { $max: "$salary" } } }])
        // console.log(data);
        data.forEach(element => {
            tConstArr.push({ tconst: element.tconst, language: element.language, region: element.region })
        });
        k = 0
        loopArray()
        function loopArray() {
            if (k < tConstArr.length) {
                bringData(() => {
                    k++;
                    loopArray()
                })
            } else {
                res.send({ status: 1, data, count })
            }
        }
        function bringData(callback) {
            var arr_L = []
            var arr_R = []
            var arr_R_final = []
            var arr_L_final = []
            var L = tConstArr[k].language
            var R = tConstArr[k].region
            L.forEach(element => {
                element == "" ? null : arr_L.push(element)
            });
            R.forEach(element => {
                element == "" ? null : arr_R.push(element)
            });
            console.log("arr_L", arr_L);
            console.log("arr_R", arr_R);
            // ja,en,mk,fr,bg
            Country.find({ code: { $in: arr_R } }, (err, coun) => {
                console.log("coun", coun);
                coun.forEach(element => {
                    arr_R_final.push(element.name)
                });
                Language.find({ code: { $in: arr_L } }, (err, lang) => {
                    console.log("lang", lang);
                    lang.forEach(element => {
                        arr_L_final.push(element.name)
                    });
                    data[k].language = arr_L_final.join(", ")
                    data[k].region = arr_R_final.join(", ")
                    callback()
                })
            })
        }
        // ********
    }
})

router.get("/original-below_6", async (req, res) => {
    var key_value_number = 0
    Merge.findOne({}, (err, merge) => {
        for (var i = 50; i > 0; i--) {
            var key = "week" + i + "_rating"
            if (merge[key] != null) {
                // i = present
                key_value_number = i - 1; // past
                break;
            }
        }
        var keyToFind = "week" + Number(key_value_number) + "_rating"
        Merge.find({ $or: [{ [keyToFind]: { $lt: "6" } }, { [keyToFind]: { $eq: "" } }] }, (err, all) => {
            res.send({ 'status': 1, 'data': all })
        })
    })
    return;
    var finalData = []
    var titleIDs = []
    var key_value_number = 1
    var allMerges = await Merge.find({})
    allMerges.forEach(element => {
        titleIDs.push(element.tconst)
    });

    k = 0;
    loopArray()
    async function loopArray() {
        if (k < titleIDs.length) {
            bringData(() => {
                k++;
                loopArray()
            })
        } else {
            res.send({ 'status': 1, data: finalData })
        }
    }
    function bringData(callback) {
        Merge.findOne({ tconst: titleIDs[k] }, (err, merge) => {
            // console.log(titleIDs[k], k, titleIDs.length);
            for (var i = 50; i > 0; i--) {
                var key = "week" + i + "_rating"
                if (merge[key] != null) {
                    // i = present
                    key_value_number = i - 1; // past
                    break;
                }
            }
            var past_key = "week" + Number(key_value_number) + "_rating"
            var past = merge[past_key]
            if (past < 6) {
                console.log(merge._id);
                finalData.push(merge)
            }
            callback()
        })
    }
})
// 03570"}}, {$project : {week1_rating : 1, week2_rating :1, sub : {$subtract : ["$convNum1", "$convNum2"]}}}]).pretty()

router.get("/original-above_500_votes", async (req, res) => {
    var key_value_number = 0
    Merge.findOne({}, (err, merge) => {
        for (var i = 50; i > 0; i--) {
            var key = "week" + i + "_votes"
            if (merge[key] != null) {
                // i = present
                key_value_number = i - 1; // past
                break;
            }
        }
        var keyToFind = "week" + Number(key_value_number) + "_rating"
        Merge.find({ $or: [{ [keyToFind]: { $lt: "6" } }, { [keyToFind]: { $eq: "" } }] }, (err, all) => {
            res.send({ 'status': 1, 'data': all })
        })
    })
    return
    var finalData = []
    var titleIDs = []
    var votes = []
    var key_value_number_votes = 1
    var allMerges = await Merge.find({})
    allMerges.forEach(element => {
        titleIDs.push(element.tconst)
        votes.push(element.numVotes)
    });

    k = 0;
    loopArray()
    async function loopArray() {
        if (k < titleIDs.length) {
            bringData(() => {
                k++;
                loopArray()
            })
        } else {
            res.send({ 'status': 1, data: finalData })
        }
    }
    function bringData(callback) {
        Merge.findOne({ tconst: titleIDs[k] }, (err, merge) => {
            for (var j = 50; j > 0; j--) {
                var key = "week" + j + "_votes"
                if (merge[key] != null) {
                    key_value_number_votes = j + 1;
                    break;
                }
            }
            var present_votes = Number(merge.numVotes)
            var past_votes = merge["week" + Number(key_value_number_votes - 2) + "_votes"]
            if (Number(present_votes - past_votes) > 500) {
                finalData.push(merge)
            }
            callback()
        })
    }
})

router.get("/merge_test", async (req, res) => {
    var M = []
    var query = [
        {
            $match: {
                $and: [{ startYear: { $gte: "2015" } }, { startYear: { $ne: "\\N" } },
                { $or: [{ "titleType": "tvMiniSeries" }, { "titleType": "tvSeries" }] }]
            }
        }, {
            $lookup: { from: 'ratings', localField: "tconst", foreignField: "tconst", as: "rate" }
        }, {
            $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "region" }
        }, {
            $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "language" }
        }, {
            $project: { startYear: 1, tconst: 1, titleType: 1, averageRating: "$rate.averageRating", numVotes: "$rate.numVotes", genres: 1, region: "$region.region", primaryTitle: 1, language: "$language.language" }
        },
        { $match: { $and: [{ averageRating: { $gte: "6" } }] } }
    ]
    var merged_data = await Title.aggregate(query)
    var titleIDs = []
    var newTitleIDs = []
    merged_data.forEach(element => {
        titleIDs.push(element.tconst)
    });
    newTitleIDs = Array.from(new Set(titleIDs))
    var merge_count = await Merge.find({})
    var mergeTitleIDs = []
    merge_count.forEach(element => {
        mergeTitleIDs.push(element.tconst)
    });
    newmergeTitleIDs = Array.from(new Set(mergeTitleIDs))
    k = 0;
    loopArray()
    async function loopArray() {
        if (k < newmergeTitleIDs.length) {
            bringData(() => {
                k++;
                loopArray()
            })
        } else {
            // var newMergeCount = await Newmerge.countDocuments()
            var num = M.length
            res.send({ data: { M, newMergeCount: num, mergecount: { merge_count: merge_count.length, mergeTitleIDCount: mergeTitleIDs.length, newmergeTitleIDCount: newmergeTitleIDs.length }, count: { mergeDataCount: merged_data.length, oldSetCount: titleIDs.length, newSetCount: newTitleIDs.length, newTitleIDs } } })
        }
    }
    function bringData(callback) {
        Merge.findOne({ tconst: newmergeTitleIDs[k] }, { __v: 0, _id: 0 }, (err, merge) => {
            M.push(merge)
            // Newmerge.create(merge, (err, newmerge) => {
            callback()
            // })
        })
    }
})

// STEP 1: CREATE CRON TABLE TO SAVE ALL ACTIVITIES DATA

// at 12 o clock mid night SATURDAY early hours
router.get("/create_cron_table", async (req, res) => {
    var new_get_settings_data = 1
    var date = new Date
    var todays_date = date.getDate() + "-" + Number(date.getMonth() + 1) + "-" + date.getFullYear()
    var allCronTables = await Cron_table.find({ operation_date: todays_date })
    var query = {
        operation_date: todays_date,
        limit: 500,
        offset: 0,
        created_on: new Date(),
        is_delete_akas_start: 0,
        is_delete_akas_end: 0,
        is_download_unzip_akas_start: 0,
        is_download_unzip_akas_end: 0,
        is_dumping_akas_start: 0,
        is_dumping_akas_end: 0,
        is_delete_title_start: 0,
        is_delete_title_end: 0,
        is_download_unzip_title_start: 0,
        is_download_unzip_title_end: 0,
        is_dumping_title_start: 0,
        is_dumping_title_end: 0,
        is_delete_rating_start: 0,
        is_delete_rating_end: 0,
        is_download_unzip_rating_start: 0,
        is_download_unzip_rating_end: 0,
        is_dumping_rating_start: 0,
        is_dumping_rating_end: 0,
        total_data_this_week: 0,
        is_delete_margin_of_1_start: 0,
        is_delete_margin_of_1_end: 0,
        is_delete_above_500_votes_start: 0,
        is_delete_above_500_votes_end: 0,
        is_delete_above_6_start: 0,
        is_delete_above_6_end: 0,
        is_delete_below_6_start: 0,
        is_delete_below_6_end: 0,
        margin_of_1: 0,
        is_merging_start: 0,
        is_merging_end: 0,
        margin_of_1: 0,
        margin_of_1_start: 0,
        margin_of_1_end: 0,
        above_500_votes: 0,
        above_500_votes_start: 0,
        above_500_votes_end: 0,
        above_6: 0,
        above_6_start: 0,
        above_6_end: 0,
        below_6: 0,
        below_6_start: 0,
        below_6_end: 0,
        is_mail_sent: 0,
    }
    if (allCronTables <= 0) {
        var get_settings_data = await Setting.findOne({})
        if (get_settings_data.is_process_complete == 1) {
            var cron = await Cron_table.create(query)
            if (get_settings_data) { new_get_settings_data = Number(get_settings_data.week_number) + 1 }
            var setting_update = await Setting.updateOne({}, { this_week_folder_name: todays_date, week_number: new_get_settings_data, is_process_complete: 0 })
        }
        res.send({ 'status': 1 })
    }
})

// STEP 2: DELETE ALL THE DATA 

// at 12:30 AM SATURDAY
router.get("/delete_akas", async (req, res) => {
    var settings = await Setting.findOne({})
    var operation_date = settings.this_week_folder_name
    var cron_query = { operation_date }
    var cron_search = await Cron_table.findOne(cron_query)
    if (!cron_search) { res.send({ 'status': 1, 'message': 'CRON TABLE NOT FOUND' }) }
    var akas_count = await AKAS.countDocuments({})
    console.log("akas_count", akas_count);
    if (akas_count > 0) {
        //continue deleting
        if ((cron_search.is_delete_akas_start == 0 && cron_search.is_delete_akas_end == 0) || (cron_search.is_delete_akas_start == 1 && cron_search.is_delete_akas_end == 0)) {
            if (cron_search.is_delete_akas_start == 0) {
                var update_cron_with = { is_delete_akas_start: 1, is_delete_akas_start_datetime: new Date() }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
            var delete_akas = await AKAS.deleteMany({})
            if (akas_count <= 0) {
                var update_cron_with = {
                    is_delete_akas_end: 1,
                    is_delete_akas_end_datetime: new Date()
                }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
        }
    } else {
        var update_cron_with = {
            is_delete_akas_start: 1,
            is_delete_akas_start_datetime: new Date(),
            is_delete_akas_end: 1,
            is_delete_akas_end_datetime: new Date()
        }
        var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
    }
    res.send({ 'status': 1 })
})
// at 12:30 AM SATURDAY
router.get("/delete_title", async (req, res) => {
    var settings = await Setting.findOne({})
    var operation_date = settings.this_week_folder_name
    var cron_query = { operation_date }
    var cron_search = await Cron_table.findOne(cron_query)
    if (!cron_search) { res.send({ 'status': 1, 'message': 'CRON TABLE NOT FOUND' }) }
    var title_count = await Title.countDocuments({})
    if (title_count > 0) {
        //continue deleting
        if ((cron_search.is_delete_title_start == 0 && cron_search.is_delete_title_end == 0) || (cron_search.is_delete_title_start == 1 && cron_search.is_delete_title_end == 0)) {
            if (cron_search.is_delete_title_start == 0) {
                var update_cron_with = { is_delete_title_start: 1, is_delete_title_start_datetime: new Date() }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
            var delete_title = await Title.deleteMany({})
            if (title_count <= 0) {
                var update_cron_with = {
                    is_delete_title_end: 1,
                    is_delete_title_end_datetime: new Date()
                }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
        }
    } else {
        var update_cron_with = {
            is_delete_title_start: 1, is_delete_title_start_datetime: new Date(),
            is_delete_title_end: 1,
            is_delete_title_end_datetime: new Date()
        }
        var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
    }
    res.send({ 'status': 1 })
})
// at 12:30 AM SATURDAY
router.get("/delete_rating", async (req, res) => {
    var settings = await Setting.findOne({})
    var operation_date = settings.this_week_folder_name
    var cron_query = { operation_date }
    var cron_search = await Cron_table.findOne(cron_query)
    if (!cron_search) { res.send({ 'status': 1, 'message': 'CRON TABLE NOT FOUND' }) }
    var rating_count = await Rating.countDocuments({})
    if (rating_count > 0) {
        //continue deleting
        if ((cron_search.is_delete_rating_start == 0 && cron_search.is_delete_rating_end == 0) || (cron_search.is_delete_rating_start == 1 && cron_search.is_delete_rating_end == 0)) {
            if (cron_search.is_delete_rating_start == 0) {
                var update_cron_with = { is_delete_rating_start: 1, is_delete_rating_start_datetime: new Date() }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
            var delete_rating = await Rating.deleteMany({})
            if (rating_count <= 0) {
                var update_cron_with = {
                    is_delete_rating_end: 1,
                    is_delete_rating_end_datetime: new Date()
                }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
        }
    } else {
        var update_cron_with = {
            is_delete_rating_start: 1, is_delete_rating_start_datetime: new Date(),
            is_delete_rating_end: 1,
            is_delete_rating_end_datetime: new Date()
        }
        var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
    }
    res.send({ 'status': 1 })
})

// STEP 3: DOWNLOAD/UNZIP FILES FOR MONGO DUMP 

// at 1 AM SATURDAY
router.get("/akas", async (req, res) => {
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    Cron_table.findOne(cron_query, async (err, cron) => {
        // continue downloading from website
        if (cron.is_download_unzip_akas_start == 1 && cron.is_download_unzip_akas_end == 0 || cron.is_download_unzip_akas_start == 0 && cron.is_download_unzip_akas_end == 0) {
            if (cron.is_download_unzip_akas_start == 0) {
                var update_query = { is_download_unzip_akas_start: 1, is_download_unzip_akas_start_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
            }
            download_upzip()
        }
        async function download_upzip() {
            var url = "https://datasets.imdbws.com/"
            var finalData = []
            var allPaths = []
            var tsvPaths = []
            const request = require('request');
            request(url, function (error, response, body) {
                var data = body.split("<ul>")
                // console.log(data);
                function manipulate(str) {
                    return str.split("href").pop().split(">").shift().split("=")[1]
                }
                for (let i = 1; i < 8; i++) {
                    finalData.push(manipulate(data[i]))
                }
                var fileUrl = finalData[1];
                var output = fileUrl.split("/").pop();
                const url = fileUrl
                var date = new Date()
                var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
                var insert_path = filepath + full_date + '/'

                if (!fs.existsSync(insert_path)) {
                    console.log("excel_file not exist");
                    shell.mkdir("-p", insert_path);
                }
                var path = insert_path + output
                var file = fs.createWriteStream(path, "utf8");
                var request = http.get(url).on('response', function (res) {
                    console.log('in cb');
                    res.on('data', function (chunk) {
                        file.write(chunk);
                        console.log(chunk);
                    }).on('end', async function () {
                        console.log("chunk-end");
                        var target_path = path.slice(0, -3)
                        const fileContents = fs.createReadStream(`${path}`)
                        const writeStream = fs.createWriteStream(`${target_path}`)
                        const unzip = zlib.createGunzip()
                        fileContents.pipe(unzip).pipe(writeStream)
                        console.log("unzip-ended");
                        var update_query = { is_download_unzip_akas_end: 1, is_download_unzip_akas_end_datetime: new Date() }
                        var update_cron = await Cron_table.updateOne(cron_query, update_query)
                    }).on('error', function (err) {
                        // clear timeout
                        console.log(err.message);
                    });
                });
            })
        }
    })
})
// at 1 AM SATURDAY
router.get("/title", async (req, res) => {
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    Cron_table.findOne(cron_query, async (err, cron) => {
        // continue downloading from website
        if (cron.is_download_unzip_title_start == 1 && cron.is_download_unzip_title_end == 0 || cron.is_download_unzip_title_start == 0 && cron.is_download_unzip_title_end == 0) {
            if (cron.is_download_unzip_title_start == 0) {
                var update_query = { is_download_unzip_title_start: 1, is_download_unzip_title_start_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
            }
            download_upzip()
        }
        async function download_upzip() {
            var url = "https://datasets.imdbws.com/"
            var finalData = []
            var allPaths = []
            var tsvPaths = []
            const request = require('request');
            request(url, function (error, response, body) {
                var data = body.split("<ul>")
                // console.log(data);
                function manipulate(str) {
                    return str.split("href").pop().split(">").shift().split("=")[1]
                }
                for (let i = 1; i < 8; i++) {
                    finalData.push(manipulate(data[i]))
                }
                var fileUrl = finalData[2];
                var output = fileUrl.split("/").pop();
                const url = fileUrl
                var date = new Date()
                var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
                var insert_path = filepath + full_date + '/'

                if (!fs.existsSync(insert_path)) {
                    console.log("excel_file not exist");
                    shell.mkdir("-p", insert_path);
                }
                var path = insert_path + output
                var file = fs.createWriteStream(path, "utf8");
                var request = http.get(url).on('response', function (res) {
                    console.log('in cb');
                    res.on('data', function (chunk) {
                        file.write(chunk);
                        console.log(chunk);
                    }).on('end', async function () {
                        console.log("chunk-end");
                        var target_path = path.slice(0, -3)
                        const fileContents = fs.createReadStream(`${path}`)
                        const writeStream = fs.createWriteStream(`${target_path}`)
                        const unzip = zlib.createGunzip()
                        fileContents.pipe(unzip).pipe(writeStream)
                        console.log("unzip-ended");
                        var update_query = { is_download_unzip_title_end: 1, is_download_unzip_title_end_datetime: new Date() }
                        var update_cron = await Cron_table.updateOne(cron_query, update_query)
                    }).on('error', function (err) {
                        // clear timeout
                        console.log(err.message);
                    });
                });
            })
        }
    })
})
// at 1 AM SATURDAY
router.get("/rating", async (req, res) => {
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    Cron_table.findOne(cron_query, async (err, cron) => {
        // continue downloading from website
        if (cron.is_download_unzip_rating_start == 1 && cron.is_download_unzip_rating_end == 0 || cron.is_download_unzip_rating_start == 0 && cron.is_download_unzip_rating_end == 0) {
            if (cron.is_download_unzip_rating_start == 0) {
                var update_query = { is_download_unzip_rating_start: 1, is_download_unzip_rating_start_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
            }
            download_upzip()
        }
        async function download_upzip() {
            var url = "https://datasets.imdbws.com/"
            var finalData = []
            var allPaths = []
            var tsvPaths = []
            const request = require('request');
            request(url, function (error, response, body) {
                var data = body.split("<ul>")
                // console.log(data);
                function manipulate(str) {
                    return str.split("href").pop().split(">").shift().split("=")[1]
                }
                for (let i = 1; i < 8; i++) {
                    finalData.push(manipulate(data[i]))
                }
                var fileUrl = finalData[6];
                var output = fileUrl.split("/").pop();
                const url = fileUrl
                var date = new Date()
                var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
                var insert_path = filepath + full_date + '/'

                if (!fs.existsSync(insert_path)) {
                    console.log("excel_file not exist");
                    shell.mkdir("-p", insert_path);
                }
                var path = insert_path + output
                var file = fs.createWriteStream(path, "utf8");
                var request = http.get(url).on('response', function (res) {
                    console.log('in cb');
                    res.on('data', function (chunk) {
                        file.write(chunk);
                        console.log(chunk);
                    }).on('end', async function () {
                        console.log("chunk-end");
                        var target_path = path.slice(0, -3)
                        const fileContents = fs.createReadStream(`${path}`)
                        const writeStream = fs.createWriteStream(`${target_path}`)
                        const unzip = zlib.createGunzip()
                        fileContents.pipe(unzip).pipe(writeStream)
                        console.log("unzip-ended");
                        var update_query = { is_download_unzip_rating_end: 1, is_download_unzip_rating_end_datetime: new Date() }
                        var update_cron = await Cron_table.updateOne(cron_query, update_query)
                    }).on('error', function (err) {
                        // clear timeout
                        console.log(err.message);
                    });
                });
            })
        }
    })
})

// STEP 4: MONGO DUMP OF ALL TABLES
// db.cron_tables.update({ "_id": ObjectId("6033fd97c58aa640ae6f92c2") }, { $set: { is_download_unzip_akas_start: 0, is_download_unzip_akas_end: 0, is_download_unzip_title_start: 0, is_download_unzip_title_end: 0, is_download_unzip_rating_start: 0, is_download_unzip_rating_end: 0 } })
// AT 4 AM SATURDAY
router.get("/download_akas", async (req, res) => {
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var cron_table = await Cron_table.findOne(cron_query)

    if (cron_table.is_download_unzip_akas_start == 1 && cron_table.is_download_unzip_akas_end == 1) {
        // dump data
        if ((cron_table.is_dumping_akas_start == 1 && cron_table.is_dumping_akas_end == 0) || (cron_table.is_dumping_akas_start == 0 && cron_table.is_dumping_akas_end == 0)) {
            if (cron_table.is_dumping_akas_start == 0) {
                var update_query = { is_dumping_akas_start: 1, is_dumping_akas_start_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
            }
            akas_dump(operation_date)
        }
    } else {
        res.send({ 'status': 1, 'message': 'AKAS download not complete yet' })
    }
    async function akas_dump(operation_date) {
        var obj = []
        var full_date = operation_date.replace(/-/g, "_");
        // var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
        var insert_path = filepath + full_date + '/' + "title.akas.tsv"
        fs.createReadStream(`${insert_path}`, "utf8")
            // .pipe()
            .on('data', async (data) => {
                var single = {}
                final_data = d3.tsvParseRows(data)
                obj = final_data.map(function (params) {
                    single = {
                        titleId: params[0] == "" || params[0] == "\\N" ? "" : params[0],
                        ordering: params[1] == "" || params[1] == "\\N" ? "" : params[1],
                        title: params[2] == "" || params[2] == "\\N" ? "" : params[2],
                        region: params[3] == "" || params[3] == "\\N" ? "" : params[3],
                        language: params[4] == "" || params[4] == "\\N" ? "" : params[4],
                        types: params[5] == "" || params[5] == "\\N" ? "" : params[5],
                        attributes: params[6] == "" || params[6] == "\\N" ? "" : params[6],
                        isOriginalTitle: params[7] == "" || params[7] == "\\N" ? "" : params[7]
                    }
                    return single
                })
                k = 0;
                loopAKASArray()
                function loopAKASArray() {
                    if (k < obj.length) {
                        insertAKASData(() => {
                            k++;
                            loopAKASArray()
                        })
                    } else {
                        return
                    }
                }
                function insertAKASData(callback_internal) {
                    if (obj[k].titleId != "" || !obj[k].titleId) {
                        var word = obj[k].titleId
                        if (word.startsWith("tt")) {
                            AKAS.insertMany(obj[k], (err, data) => {
                                callback_internal()
                            })
                        }
                    }
                    callback_internal()
                }
            })
            .on('end', async () => {
                var update_query = { is_dumping_akas_end: 1, is_dumping_akas_end_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
                // return
            })
    }
})
// AT 4 AM SATURDAY
router.get("/download_title", async (req, res) => {
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var cron_table = await Cron_table.findOne(cron_query)

    if (cron_table.is_download_unzip_title_start == 1 && cron_table.is_download_unzip_title_end == 1) {
        // dump data
        if ((cron_table.is_dumping_title_start == 1 && cron_table.is_dumping_title_end == 0) || (cron_table.is_dumping_title_start == 0 && cron_table.is_dumping_title_end == 0)) {
            if (cron_table.is_dumping_title_start == 0) {
                var update_query = { is_dumping_title_start: 1, is_dumping_title_start_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
            }
            title_dump(operation_date)
        }
    } else {
        res.send({ 'status': 1, 'message': 'Title download not complete yet' })
    }
    async function title_dump(operation_date) {
        var obj = []
        var final_data = []
        var dataToAdd = []
        var full_date = operation_date.replace(/-/g, "_");
        // var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
        var insert_path = filepath + full_date + '/' + "title.basics.tsv"
        fs.createReadStream(`${insert_path}`, "utf8")
            // .pipe()
            .on('data', (data) => {
                final_data = d3.tsvParseRows(data)
                obj = final_data.map(function (params) {
                    return {
                        tconst: !params[0] || params[0] == "" || params[0] == "\N" ? "" : params[0],
                        titleType: !params[1] || params[1] == "" || params[1] == "\N" ? "" : params[1],
                        primaryTitle: !params[2] || params[2] == "" || params[2] == "\N" ? "" : params[2],
                        originalTitle: !params[3] || params[3] == "" || params[3] == "\N" ? "" : params[3],
                        isAdult: !params[4] || params[4] == "" || params[4] == "\N" ? "" : params[4],
                        startYear: !params[5] || params[5] == "" || params[5] == "\N" ? "" : params[5],
                        endYear: !params[6] || params[6] == "" || params[6] == "\N" ? "" : params[6],
                        runtimeMinutes: !params[7] || params[7] == "" || params[7] == "\N" ? "" : params[7],
                        genres: !params[8] || params[8] == "" || params[8] == "\N" ? "" : params[8]
                    }
                })
                k = 0;
                loopArray()
                function loopArray() {
                    if (k < obj.length) {
                        insertData(() => {
                            k++;
                            loopArray()
                        })
                    } else {
                        return;
                    }
                }
                function insertData(callback) {

                    if (obj[k].tconst != "" || !obj[k].tconst) {
                        var word = obj[k].tconst
                        if (word.startsWith("tt")) {
                            Title.insertMany(obj[k], (err, data) => {
                                callback()
                            })
                        }
                    }
                    callback()
                }
            })
            .on('end', async () => {
                var update_query = { is_dumping_title_end: 1, is_dumping_title_end_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
                // return
            })
    }
})
// AT 4 AM SATURDAY
router.get("/download_rating", async (req, res) => {
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var cron_table = await Cron_table.findOne(cron_query)

    if (cron_table.is_download_unzip_rating_start == 1 && cron_table.is_download_unzip_rating_end == 1) {
        // dump data
        if ((cron_table.is_dumping_rating_start == 1 && cron_table.is_dumping_rating_end == 0) || (cron_table.is_dumping_rating_start == 0 && cron_table.is_dumping_rating_end == 0)) {
            if (cron_table.is_dumping_rating_start == 0) {
                var update_query = { is_dumping_rating_start: 1, is_dumping_rating_start_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
            }
            rating_dump(operation_date)
        }
    } else {
        res.send({ 'status': 1, 'message': 'Rating download not complete yet' })
    }
    async function rating_dump(operation_date) {
        var obj = []
        var final_data = []
        var dataToAdd = []
        var full_date = operation_date.replace(/-/g, "_");
        // var full_date = date.getDate() + '_' + Number(date.getMonth() + 1) + '_' + date.getFullYear()
        var insert_path = filepath + full_date + '/' + "title.ratings.tsv"
        fs.createReadStream(`${insert_path}`, "utf8")
            .on('data', async (data) => {
                final_data = d3.tsvParseRows(data)
                obj = final_data.map(function (params) {
                    return {
                        tconst: params[0] == "" || params[0] == "\\N" ? "" : params[0],
                        averageRating: params[1] == "" || params[1] == "\\N" ? "" : params[1],
                        numVotes: params[2] == "" || params[2] == "\\N" ? "" : params[2],
                    }
                })
                k = 0;
                loopRatingArray()
                function loopRatingArray() {
                    if (k < obj.length) {
                        insertRatingData(() => {
                            k++;
                            loopRatingArray()
                        })
                    } else {
                        return;
                    }
                }
                function insertRatingData(callback_internal) {
                    if (obj[k].tconst != "" || !obj[k].tconst) {
                        var word = obj[k].tconst
                        if (word.startsWith("tt")) {
                            Rating.insertMany(obj[k], (err, data) => {
                                callback_internal()
                            })
                        }
                    }
                    callback_internal()
                }
            })
            .on('end', async () => {
                var update_query = { is_dumping_rating_end: 1, is_dumping_rating_end_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
                // return
            })
    }
})
// AT 10 AM SATURDAY
router.get("/delete_margin_of_1", async (req, res) => {
    var settings = await Setting.findOne({})
    var operation_date = settings.this_week_folder_name
    var cron_query = { operation_date }
    var cron_search = await Cron_table.findOne(cron_query)
    if (!cron_search) { res.send({ 'status': 1, 'message': 'CRON TABLE NOT FOUND' }) }
    var margin_of_1_count = await Margin_of_1.countDocuments({})
    if (margin_of_1_count > 0) {
        //continue deleting
        if ((cron_search.is_delete_margin_of_1_start == 0 && cron_search.is_delete_margin_of_1_end == 0) || (cron_search.is_delete_margin_of_1_start == 1 && cron_search.is_delete_margin_of_1_end == 0)) {
            if (cron_search.is_delete_margin_of_1_start == 0) {
                var update_cron_with = { is_delete_margin_of_1_start: 1, is_delete_margin_of_1_start_datetime: new Date() }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
            var delete_margin_of_1 = await Margin_of_1.deleteMany({})
            if (margin_of_1_count <= 0) {
                var update_cron_with = {
                    is_delete_margin_of_1_end: 1,
                    is_delete_margin_of_1_end_datetime: new Date()
                }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
        }
    } else {
        var update_cron_with = {
            is_delete_margin_of_1_start: 1, is_delete_margin_of_1_start_datetime: new Date(),
            is_delete_margin_of_1_end: 1,
            is_delete_margin_of_1_end_datetime: new Date()
        }
        var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
    }
    res.send({ 'status': 1 })
})
// AT 10 AM SATURDAY
router.get("/delete_above_500_votes", async (req, res) => {
    var settings = await Setting.findOne({})
    var operation_date = settings.this_week_folder_name
    var cron_query = { operation_date }
    var cron_search = await Cron_table.findOne(cron_query)
    if (!cron_search) { res.send({ 'status': 1, 'message': 'CRON TABLE NOT FOUND' }) }
    var above_500_votes_count = await Above_500_votes.countDocuments({})
    if (above_500_votes_count > 0) {
        //continue deleting
        if ((cron_search.is_delete_above_500_votes_start == 0 && cron_search.is_delete_above_500_votes_end == 0) || (cron_search.is_delete_above_500_votes_start == 1 && cron_search.is_delete_above_500_votes_end == 0)) {
            if (cron_search.is_delete_above_500_votes_start == 0) {
                var update_cron_with = { is_delete_above_500_votes_start: 1, is_delete_above_500_votes_start_datetime: new Date() }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
            var delete_above_500_votes = await Above_500_votes.deleteMany({})
            if (above_500_votes_count <= 0) {
                var update_cron_with = {
                    is_delete_above_500_votes_end: 1,
                    is_delete_above_500_votes_end_datetime: new Date()
                }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
        }
    } else {
        var update_cron_with = {
            is_delete_above_500_votes_start: 1, is_delete_above_500_votes_start_datetime: new Date(),
            is_delete_above_500_votes_end: 1,
            is_delete_above_500_votes_end_datetime: new Date()
        }
        var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
    }
    res.send({ 'status': 1 })
})
// AT 10 AM SATURDAY
router.get("/delete_above_6", async (req, res) => {
    var settings = await Setting.findOne({})
    var operation_date = settings.this_week_folder_name
    var cron_query = { operation_date }
    var cron_search = await Cron_table.findOne(cron_query)
    if (!cron_search) { res.send({ 'status': 1, 'message': 'CRON TABLE NOT FOUND' }) }
    var above_6_count = await Above_6.countDocuments({})
    if (above_6_count > 0) {
        //continue deleting
        if ((cron_search.is_delete_above_6_start == 0 && cron_search.is_delete_above_6_end == 0) || (cron_search.is_delete_above_6_start == 1 && cron_search.is_delete_above_6_end == 0)) {
            if (cron_search.is_delete_above_6_start == 0) {
                var update_cron_with = { is_delete_above_6_start: 1, is_delete_above_6_start_datetime: new Date() }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
            var delete_above_6 = await Above_6.deleteMany({})
            if (above_6_count <= 0) {
                var update_cron_with = {
                    is_delete_above_6_end: 1,
                    is_delete_above_6_end_datetime: new Date()
                }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
        }
    } else {
        var update_cron_with = {
            is_delete_above_6_start: 1, is_delete_above_6_start_datetime: new Date(),
            is_delete_above_6_end: 1,
            is_delete_above_6_end_datetime: new Date()
        }
        var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
    }
    res.send({ 'status': 1 })
})
// AT 10 AM SATURDAY
router.get("/delete_below_6", async (req, res) => {
    var settings = await Setting.findOne({})
    var operation_date = settings.this_week_folder_name
    var cron_query = { operation_date }
    var cron_search = await Cron_table.findOne(cron_query)
    if (!cron_search) { res.send({ 'status': 1, 'message': 'CRON TABLE NOT FOUND' }) }
    var below_6_count = await Below_6.countDocuments({})
    if (below_6_count > 0) {
        //continue deleting
        if ((cron_search.is_delete_below_6_start == 0 && cron_search.is_delete_below_6_end == 0) || (cron_search.is_delete_below_6_start == 1 && cron_search.is_delete_below_6_end == 0)) {
            if (cron_search.is_delete_below_6_start == 0) {
                var update_cron_with = { is_delete_below_6_start: 1, is_delete_below_6_start_datetime: new Date() }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
            var delete_below_6 = await Below_6.deleteMany({})
            if (below_6_count <= 0) {
                var update_cron_with = {
                    is_delete_below_6_end: 1,
                    is_delete_below_6_end_datetime: new Date()
                }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
        }
    } else {
        var update_cron_with = {
            is_delete_below_6_start: 1, is_delete_below_6_start_datetime: new Date(),
            is_delete_below_6_end: 1,
            is_delete_below_6_end_datetime: new Date()
        }
        var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
    }
    res.send({ 'status': 1 })
})

// STEP 5: MERGE THE DATA FOR ANALYSIS

// AT 4 PM every 10 minutes
// if data is 1L then total time taken will be 2000 minutes ie; appx 33 hours
router.get("/merge", async (req, res) => {
    var merge_data = []
    var votes = []
    var ratings = []
    var titleID = []
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var cron_table = await Cron_table.findOne(cron_query)
    var limit = cron_table.limit
    var offset = cron_table.offset
    var present_week_number = setting.week_number

    if (cron_table.is_dumping_rating_end == 1 && cron_table.is_dumping_akas_end == 1 && cron_table.is_dumping_title_end == 1) {
        if ((cron_table.is_merging_start == 1 && cron_table.is_merging_end == 0) || (cron_table.is_merging_start == 0 && cron_table.is_merging_end == 0)) {
            if (cron_table.is_merging_start == 0) {
                var update_query = { is_merging_start: 1, is_merging_start_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
            }
            merge()
        }
    }
    async function merge() {
        var query = [
            {
                $match: {
                    $and: [{ startYear: { $gte: "2015" } }, { startYear: { $ne: "\\N" } },
                    { $or: [{ "titleType": "tvMiniSeries" }, { "titleType": "tvSeries" }] }]
                }
            }, {
                $lookup: { from: 'ratings', localField: "tconst", foreignField: "tconst", as: "rate" }
            }, {
                $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "region" }
            }, {
                $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "language" }
            }, {
                $project: { startYear: 1, tconst: 1, titleType: 1, averageRating: "$rate.averageRating", numVotes: "$rate.numVotes", genres: 1, region: "$region.region", primaryTitle: 1, language: "$language.language" }
            },
            // { $match: { $and: [{ averageRating: { $gte: "6" } }] } },
            { $skip: offset },
            { $limit: limit },
        ]
        Title.aggregate(query, async (err, data) => {
            console.log("err, data", err, data.length);
            if (cron_table.total_data_this_week == 0) {
                var update_cron_query = { total_data_this_week: data.length }
                var update_cron = await Cron_table.updateOne(cron_query, update_cron_query)
            }
            // return;
            if (data.length > 0) {
                // above_6 = data.length
                data.forEach(element => {
                    titleID.push(element.tconst)
                    ratings.push(element.averageRating[0])
                    votes.push(element.numVotes[0])
                });
                k = 0;
                loopArray()
                async function loopArray() {
                    if (k < data.length) {
                        bringData(() => {
                            k++;
                            loopArray()
                        })
                    } else {
                        if (merge_data.length > 0) {
                            var merge_create = await Merge.insertMany(merge_data)
                        }
                        if (offset > cron_table.total_data_this_week) {
                            //end
                            var update_query = { is_merging_end: 1, is_merging_end_datetime: new Date() }
                            var update_cron = await Cron_table.updateOne(cron_query, update_query)
                        } else {
                            var newOffset = Number(offset) + 500
                            var update_cron = await Cron_table.updateOne(cron_query, { offset: newOffset })
                        }
                    }
                }
                async function bringData(callback) {
                    var numVotes = []
                    var averageRating = []

                    Merge.findOne({ tconst: titleID[k] }, async (err, merge) => {
                        console.log("titleID[k]", titleID[k]);
                        var rating_week_name = "week" + present_week_number + "_rating"
                        var votes_week_name = "week" + present_week_number + "_votes"
                        var week_name = "week" + present_week_number
                        if (merge) {
                            averageRating.push(ratings[k])
                            numVotes.push(votes[k])
                            var d = new Date();
                            var data_to_update = {
                                [rating_week_name]: ratings[k], [votes_week_name]: votes[k], averageRating, numVotes, [week_name]: new Date()
                            }
                            var update_merge = await Merge.updateOne({ tconst: titleID[k] }, data_to_update)
                        } else {
                            data[k][rating_week_name] = ratings[k]
                            data[k][votes_week_name] = votes[k]
                            data[k][week_name] = new Date()

                            merge_data.push(data[k])
                        }
                        callback()
                    })
                }
            } else {
                // compare and send email
                if (cron.is_merge_done == 0 || !cron.is_merge_done || cron.is_merge_done == "" || cron.is_merge_done == "0") {
                    Cron_table.updateOne({ operation_date: operationDates[l] }, { is_merge_done: 1 }, (err, up_cron) => {
                        callback_cron()
                    })
                } else {
                    callback_cron()
                }
            }
        })
    }
})

// STEP 6: ANALYSE THE DATA
// MONDAY 4 PM (AFTER 36 HOURS)
router.get("/margin_of_1", async (req, res) => {
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var cron_table = await Cron_table.findOne(cron_query)
    var tConstArr = []
    // margin_of_1_start: Number,
    // margin_of_1_start_datetime: Date,
    // margin_of_1_end: Number,
    // margin_of_1_end_datetime: Date,
    Margin_of_1.find({}, async (err, data) => {
        if (err) {
            res.send({ status: 0, message: "Failed to get data" })
        } else {
            if (data.length > 0) {
                data.forEach(element => {
                    tConstArr.push({ tconst: element.tconst, language: element.language, region: element.region })
                });
                var lang_arr = await Language.find({}, { __v: 0, _id: 0 })
                var coun_arr = await Country.find({}, { __v: 0, _id: 0 })
                k = 0
                loopArray()
                function loopArray() {
                    if (k < tConstArr.length) {
                        bringData(() => {
                            k++;
                            loopArray()
                        })
                    } else {
                        res.send({ status: 1, data, count: data.length })
                    }
                }
                function bringData(callback) {
                    var arr_L = []
                    var arr_R = []
                    var arr_R_final = []
                    var arr_L_final = []
                    var L = tConstArr[k].language
                    var R = tConstArr[k].region
                    L.forEach(element => {
                        element == "" ? null : arr_L.push(element)
                    });
                    R.forEach(element => {
                        element == "" ? null : arr_R.push(element)
                    });
                    // *******
                    for (let i = 0; i < arr_L.length; i++) {
                        lang_arr.find(element => element.code == arr_L[i] ? arr_L_final.push(element.name) : null)
                    }
                    for (let i = 0; i < arr_R.length; i++) {
                        coun_arr.find(element => element.code == arr_R[i] ? arr_R_final.push(element.name) : null)
                    }
                    data[k].language = arr_L_final.join(", ")
                    data[k].region = arr_R_final.join(", ")
                    callback()
                    return;
                    // *******
                    // ja,en,mk,fr,bg
                    Country.find({ code: { $in: arr_R } }, (err, coun) => {
                        // console.log("coun", coun);
                        coun.forEach(element => {
                            arr_R_final.push(element.name)
                        });
                        Language.find({ code: { $in: arr_L } }, (err, lang) => {
                            // console.log("lang", lang);
                            lang.forEach(element => {
                                arr_L_final.push(element.name)
                            });
                            data[k].language = arr_L_final.join(", ")
                            data[k].region = arr_R_final.join(", ")
                            callback()
                        })
                    })
                }
            } else {
                res.send({ 'status': 1, data: [] })
            }
        }
    })
    return false;
    if ((cron_table.margin_of_1_start == 0 && cron_table.margin_of_1_end == 0) && (cron_table.margin_of_1_start == 1 && cron_table.margin_of_1_end == 0)) {
        if (cron_table.margin_of_1_start == 0) {
            var update_query = { margin_of_1_start: 1, margin_of_1_start_datetime: new Date() }
            var update_cron = await Cron_table.updateOne(cron_query, update_query)
        } else {
            if (cron_table.week_number == 1) {
                var update_query = { margin_of_1: 0, margin_of_1_end: 1, margin_of_1_end_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
            } else {
                var present = cron_table.week_number
                var past = Number(cron_table.week_number) - 1

                var rating_present = "week" + present + "_rating"
                var rating_past = "week" + past + "_rating"

                // ****
                var titleIDs = []
                var ratings = []
                var votes = []
                var margin_of_1 = 0

                var allMerges = await Merge.find({})

                allMerges.forEach(element => {
                    titleIDs.push(element.tconst)
                    votes.push(element.numVotes)
                    ratings.push(element.averageRating)
                });

                k = 0;
                loopArray()
                async function loopArray() {
                    if (k < titleIDs.length) {
                        bringData(() => {
                            k++;
                            loopArray()
                        })
                    } else {
                        var update_query = { margin_of_1, margin_of_1_end: 1, margin_of_1_end_datetime: new Date() }
                        var update_cron = await Cron_table.updateOne(cron_query, update_query)
                    }
                }
                async function bringData(callback) {
                    Merge.findOne({ tconst: titleIDs[k] }, async (err, merge) => {
                        console.log(titleIDs[k], k, titleIDs.length);

                        var present = merge[rating_present]
                        var past = merge[rating_past]

                        if (Number(present - past) > 1) {
                            margin_of_1++;
                            var create_merge = await Margin_of_1.create(merge)
                        }
                        callback()
                    })
                }
                // ****

            }
        }
    }
})

// MONDAY 4 PM (AFTER 36 HOURS)
router.get("/below_6", async (req, res) => {
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var cron_table = await Cron_table.findOne(cron_query)
    var tConstArr = []
    if (setting.is_process_complete == 1) {
        var week_number = setting.week_number
    } else {
        var week_number = Number(setting.week_number - 1)
    }
    var present = week_number
    var past = week_number - 1

    var rating_present = "week" + present + "_rating"
    var rating_past = "week" + past + "_rating"
    Merge.find({ [rating_present]: { $exists: false } }, async (err, data) => {
        if (err) {
            res.send({ 'status': 0, message: err.stack, data: [] })
        } else {
            if (data.length > 0) {
                // res.send({ 'status': 1, data: merge })
                data.forEach(element => {
                    tConstArr.push({ tconst: element.tconst, language: element.language, region: element.region })
                });
                var lang_arr = await Language.find({}, { __v: 0, _id: 0 })
                var coun_arr = await Country.find({}, { __v: 0, _id: 0 })
                k = 0
                loopArray()
                function loopArray() {
                    if (k < tConstArr.length) {
                        bringData(() => {
                            k++;
                            loopArray()
                        })
                    } else {
                        res.send({ status: 1, data, count: data.length })
                    }
                }
                function bringData(callback) {
                    var arr_L = []
                    var arr_R = []
                    var arr_R_final = []
                    var arr_L_final = []
                    var L = tConstArr[k].language
                    var R = tConstArr[k].region
                    L.forEach(element => {
                        element == "" ? null : arr_L.push(element)
                    });
                    R.forEach(element => {
                        element == "" ? null : arr_R.push(element)
                    });
                    // *******
                    for (let i = 0; i < arr_L.length; i++) {
                        lang_arr.find(element => element.code == arr_L[i] ? arr_L_final.push(element.name) : null)
                    }
                    for (let i = 0; i < arr_R.length; i++) {
                        coun_arr.find(element => element.code == arr_R[i] ? arr_R_final.push(element.name) : null)
                    }
                    data[k].language = arr_L_final.join(", ")
                    data[k].region = arr_R_final.join(", ")
                    callback()
                    return;
                    // *******
                    // ja,en,mk,fr,bg
                    Country.find({ code: { $in: arr_R } }, (err, coun) => {
                        // console.log("coun", coun);
                        coun.forEach(element => {
                            arr_R_final.push(element.name)
                        });
                        Language.find({ code: { $in: arr_L } }, (err, lang) => {
                            // console.log("lang", lang);
                            lang.forEach(element => {
                                arr_L_final.push(element.name)
                            });
                            data[k].language = arr_L_final.join(", ")
                            data[k].region = arr_R_final.join(", ")
                            callback()
                        })
                    })
                }
            } else {
                res.send({ 'status': 1, data: [] })
            }
        }
    })
    return;
    Below_6.find({}, (err, data) => {
        if (err) {
            res.send({ status: 0, message: "Failed to get data" })
        }
        if (data) {
            res.send({ status: 1, data: data })
        } else {
            res.send({ status: 1, data: [] })
        }
    })
    return false;
    if ((cron_table.below_6_start == 0 && cron_table.below_6_end == 0) && (cron_table.below_6_start == 1 && cron_table.below_6_end == 0)) {
        if (cron_table.below_6_start == 0) {
            var update_query = { below_6_start: 1, below_6_start_datetime: new Date() }
            var update_cron = await Cron_table.updateOne(cron_query, update_query)
        } else {
            if (cron_table.week_number == 1) {
                var update_query = { below_6: 0, below_6_end: 1, below_6_end_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
            } else {
                var present = cron_table.week_number
                var past = Number(cron_table.week_number) - 1

                var rating_present = "week" + present + "_rating"
                var rating_past = "week" + past + "_rating"

                // ****
                var titleIDs = []
                var ratings = []
                var votes = []
                var below_6 = 0

                var allMerges = await Merge.find({})

                allMerges.forEach(element => {
                    titleIDs.push(element.tconst)
                    votes.push(element.numVotes)
                    ratings.push(element.averageRating)
                });

                k = 0;
                loopArray()
                async function loopArray() {
                    if (k < titleIDs.length) {
                        bringData(() => {
                            k++;
                            loopArray()
                        })
                    } else {
                        var update_query = { below_6, below_6_end: 1, below_6_end_datetime: new Date() }
                        var update_cron = await Cron_table.updateOne(cron_query, update_query)
                    }
                }
                async function bringData(callback) {
                    Merge.findOne({ tconst: titleIDs[k] }, async (err, merge) => {
                        console.log(titleIDs[k], k, titleIDs.length);

                        var present = merge[rating_present]
                        var past = merge[rating_past]

                        if (present < 6 && past > 6) {
                            below_6++;
                            var create_merge = await Below_6.create(merge)
                        }
                        callback()
                    })
                }
                // ****

            }
        }
    }
})

// MONDAY 4 PM (AFTER 36 HOURS)
router.get("/above_500_votes", async (req, res) => {
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var cron_table = await Cron_table.findOne(cron_query)
    var tConstArr = []
    var present = cron_table.week_number
    Above_500_votes.find({}, async (err, data) => {
        if (err) {
            res.send({ status: 0, message: "Failed to get data" })
        } else {
            if (data.length > 0) {
                // res.send({ status: 1, data: data })
                data.forEach(element => {
                    tConstArr.push({ tconst: element.tconst, language: element.language, region: element.region })
                });
                var lang_arr = await Language.find({}, { __v: 0, _id: 0 })
                var coun_arr = await Country.find({}, { __v: 0, _id: 0 })
                k = 0
                loopArray()
                function loopArray() {
                    if (k < tConstArr.length) {
                        bringData(() => {
                            k++;
                            loopArray()
                        })
                    } else {
                        res.send({ status: 1, data, count: data.length })
                    }
                }
                function bringData(callback) {
                    var arr_L = []
                    var arr_R = []
                    var arr_R_final = []
                    var arr_L_final = []
                    var L = tConstArr[k].language
                    var R = tConstArr[k].region
                    L.forEach(element => {
                        element == "" ? null : arr_L.push(element)
                    });
                    R.forEach(element => {
                        element == "" ? null : arr_R.push(element)
                    });
                    // *******
                    for (let i = 0; i < arr_L.length; i++) {
                        lang_arr.find(element => element.code == arr_L[i] ? arr_L_final.push(element.name) : null)
                    }
                    for (let i = 0; i < arr_R.length; i++) {
                        coun_arr.find(element => element.code == arr_R[i] ? arr_R_final.push(element.name) : null)
                    }
                    data[k].language = arr_L_final.join(", ")
                    data[k].region = arr_R_final.join(", ")
                    callback()
                    return;
                    // *******
                    // ja,en,mk,fr,bg
                    Country.find({ code: { $in: arr_R } }, (err, coun) => {
                        // console.log("coun", coun);
                        coun.forEach(element => {
                            arr_R_final.push(element.name)
                        });
                        Language.find({ code: { $in: arr_L } }, (err, lang) => {
                            // console.log("lang", lang);
                            lang.forEach(element => {
                                arr_L_final.push(element.name)
                            });
                            data[k].language = arr_L_final.join(", ")
                            data[k].region = arr_R_final.join(", ")
                            callback()
                        })
                    })
                }

            } else {
                res.send({ status: 1, data: [] })
            }
        }
    })
    return false;
    if (present == 1) {
        var update_query = { above_500_votes: 0, above_500_votes_end: 1, above_500_votes_end_datetime: new Date() }
        var update_cron = await Cron_table.updateOne(cron_query, update_query)
    } else {
        var past = Number(cron_table.week_number) - 1

        var votes_present = "week" + present + "_votes"
        var rating_present = "week" + present + "_rating"

        var votes_past = "week" + past + "_votes"

        var titleIDs = []
        var votes = []
        var above_500_votes = 0

        var allMerges = await Merge.find({ [rating_present]: { $gte: "6" } })

        allMerges.forEach(element => {
            titleIDs.push(element.tconst)
            votes.push(element.numVotes)
        });

        k = 0;
        loopArray()
        async function loopArray() {
            if (k < titleIDs.length) {
                bringData(() => {
                    k++;
                    loopArray()
                })
            } else {
                var update_query = { above_500_votes, above_500_votes_end: 1, above_500_votes_end_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
            }
        }
        async function bringData(callback) {
            Merge.findOne({ tconst: titleIDs[k] }, async (err, merge) => {
                console.log(titleIDs[k], k, titleIDs.length);

                var present = merge[votes_present]
                var past = merge[votes_past]

                if (Number(past - present) >= 500) {
                    above_500_votes++;
                    var create_merge = await Above_500_votes.create(merge)
                }
                callback()
            })
        }
    }
})

// MONDAY 4 PM (AFTER 36 HOURS)
router.get("/above_6", async (req, res) => {
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var cron_table = await Cron_table.findOne(cron_query)
    var tConstArr = []
    // var above_6 = await Merge.find({ [rating_past]: { $exists: false } }).count()
    // var below_6 = await Merge.find({ $and: [{ [rating_past]: { $exists: true } }, { [rating_present]: { $exists: false } }] }).count()

    if (setting.is_process_complete == 1) {
        var week_number = setting.week_number
    } else {
        var week_number = Number(setting.week_number - 1)
    }
    var present = week_number
    var past = week_number - 1

    var rating_present = "week" + present + "_rating"
    var rating_past = "week" + past + "_rating"
    // var query = [
    //     { $match: { $and: [{ averageRating: { $lte: "7" } }, { [rating_past]: { $exists: false } }, { [rating_present]: { $exists: true } }] } },
    // ]
    Merge.find({ $and: [{ [rating_past]: { $exists: false } }, { [rating_present]: { $exists: true } }] }, async (err, data) => {
        if (err) {
            res.send({ status: 0, data: [], message: err.stack })
        } else {
            if (data.length > 0) {
                // res.send({ status: 1, data: merge })
                data.forEach(element => {
                    tConstArr.push({ tconst: element.tconst, language: element.language, region: element.region })
                });
                var lang_arr = await Language.find({}, { __v: 0, _id: 0 })
                var coun_arr = await Country.find({}, { __v: 0, _id: 0 })
                k = 0
                loopArray()
                function loopArray() {
                    if (k < tConstArr.length) {
                        bringData(() => {
                            k++;
                            loopArray()
                        })
                    } else {
                        res.send({ status: 1, data, count: data.length })
                    }
                }
                function bringData(callback) {
                    var arr_L = []
                    var arr_R = []
                    var arr_R_final = []
                    var arr_L_final = []
                    var L = tConstArr[k].language
                    var R = tConstArr[k].region
                    L.forEach(element => {
                        element == "" ? null : arr_L.push(element)
                    });
                    R.forEach(element => {
                        element == "" ? null : arr_R.push(element)
                    });
                    // *******
                    for (let i = 0; i < arr_L.length; i++) {
                        lang_arr.find(element => element.code == arr_L[i] ? arr_L_final.push(element.name) : null)
                    }
                    for (let i = 0; i < arr_R.length; i++) {
                        coun_arr.find(element => element.code == arr_R[i] ? arr_R_final.push(element.name) : null)
                    }
                    data[k].language = arr_L_final.join(", ")
                    data[k].region = arr_R_final.join(", ")
                    callback()
                    return;
                    // *******
                    // ja,en,mk,fr,bg
                    Country.find({ code: { $in: arr_R } }, (err, coun) => {
                        // console.log("coun", coun);
                        coun.forEach(element => {
                            arr_R_final.push(element.name)
                        });
                        Language.find({ code: { $in: arr_L } }, (err, lang) => {
                            // console.log("lang", lang);
                            lang.forEach(element => {
                                arr_L_final.push(element.name)
                            });
                            data[k].language = arr_L_final.join(", ")
                            data[k].region = arr_R_final.join(", ")
                            callback()
                        })
                    })
                }
            } else {
                res.send({ status: 1, data: [] })
            }
        }

    })
    return;
    Above_6.find({}, (err, data) => {
        if (err) {
            res.send({ status: 0, message: "Failed to get data" })
        }
        if (data) {
            res.send({ status: 1, data: data })
        } else {
            res.send({ status: 1, data: [] })
        }
    })
    return false;
    if ((cron_table.above_6_start == 0 && cron_table.above_6_end == 0) && (cron_table.above_6_start == 1 && cron_table.above_6_end == 0)) {
        if (cron_table.above_6_start == 0) {
            var update_query = { above_6_start: 1, above_6_start_datetime: new Date() }
            var update_cron = await Cron_table.updateOne(cron_query, update_query)
        } else {
            if (cron_table.week_number == 1) {
                var update_query = { above_6: 0, above_6_end: 1, above_6_end_datetime: new Date() }
                var update_cron = await Cron_table.updateOne(cron_query, update_query)
            } else {
                var present = cron_table.week_number
                var past = Number(cron_table.week_number) - 1

                var rating_present = "week" + present + "_rating"
                var rating_past = "week" + past + "_rating"

                // ****
                var titleIDs = []
                var ratings = []
                var votes = []
                var above_6 = 0

                var allMerges = await Merge.find({})

                allMerges.forEach(element => {
                    titleIDs.push(element.tconst)
                    votes.push(element.numVotes)
                    ratings.push(element.averageRating)
                });

                k = 0;
                loopArray()
                async function loopArray() {
                    if (k < titleIDs.length) {
                        bringData(() => {
                            k++;
                            loopArray()
                        })
                    } else {
                        var update_query = { above_6, above_6_end: 1, above_6_end_datetime: new Date() }
                        var update_cron = await Cron_table.updateOne(cron_query, update_query)
                    }
                }
                async function bringData(callback) {
                    Merge.findOne({ tconst: titleIDs[k] }, async (err, merge) => {
                        console.log(titleIDs[k], k, titleIDs.length);

                        var present = merge[rating_present]
                        var past = merge[rating_past]

                        if (past < 6 && present > 6) {
                            above_6++;
                            var create_merge = await Above_6.create(merge)
                        }
                        callback()
                    })
                }
                // ****

            }
        }
    }
})

// STEP 6: SEND ALERT EMAIL 

// MONDAY 8 PM 
router.get("/send_email", async (req, res) => {
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var cron_table = await Cron_table.findOne(cron_query)

    if (cron_table.margin_of_1_end == 1 && cron_table.above_500_votes_end == 1 && cron_table.above_6_end == 1 && cron_table.below_6_end == 1) {
        if (is_mail_sent == 1) { // don't send
            res.send({ 'status': 1, 'message': 'Email already sent' })
        } else {
            var emailtest = {
                "fromEmail": "noreply@flickquickapp.com",
                "fromName": "FLICKQUICK",
                "hostname": "smtp.sparkpostmail.com",
                "userName": "SMTP_Injection",
                "apiKey": "bd99cb8ff7487d7c3e89befe064ee3a6e3a6d443"
            }
            const nodemailer = require("nodemailer");
            var transporter = nodemailer.createTransport({
                host: emailtest.hostname,
                port: 587,
                secure: false, // true for 465, false for other ports
                auth: {
                    user: emailtest.userName,
                    pass: emailtest.apiKey
                }
            });
            var mailOptions = "";
            link =
                "<p>ALERTS REQUIRED BY AQUISITION TEAM</p><br><p>The following are the alerts raised for IMDb data analysis</p><br><ul><li><a href = 'http://139.59.18.134:5000/emaildata?id=1'>All Shows that have increased from Above 6 by a margin of 1 full point in ratings : " + cron_table.margin_of_1 + "</a></li><li><a href = 'http://139.59.18.134:5000/emaildata?id=2'> All Shows that have increased from Above 6 and an increase of atleast 500 votes: " + cron_table.above_500_votes + "</a></li><li><a href = 'http://139.59.18.134:5000/emaildata?id=3'>All Shows that have decreased their rating from above 6 to below 6 : " + cron_table.below_6 + "</a></li><li><a href = 'http://139.59.18.134:5000/emaildata?id=4'>All Shows that have increased their rating from below 6 to above 6 : " + cron_table.above_6 + "</a></li></ul>";
            mailOptions = {
                from: emailtest.fromEmail,
                to: "harapriya.akella@gmail.com",
                subject: "ALERTS EMAIL",
                html: link
            };
            transporter.sendMail(mailOptions, async function (error, info) {
                if (!error) {
                    var send_email = await Cron_table.updateOne(cron_query, { is_mail_sent: 1, is_mail_sent_on: new Date() })
                    var setting_update = await Setting.updateOne({}, { is_process_complete: 1 })
                }
            });
        }
    }
})

router.get("/update_merge", async (req, res) => {
    console.log("START");
    var page = Number(req.query.page)
    var limit = 10000
    var skip = Number(limit * (page - 1))
    var all_merge = await Merge.find({}, { tconst: 1 }).limit(limit).skip(skip)
    k = 0
    loopArray()
    function loopArray() {
        if (k < all_merge.length) {
            bringData(() => {
                k++;
                loopArray()
            })
        } else {
            console.log("DONE");
            res.send({ status: 1, message: 'Done' })
        }
    }
    async function bringData(callback) {
        var main_language = ""
        var main_country = ""
        var titleId = all_merge[k].tconst
        console.log("titleId", titleId);
        var findAKAS = await AKAS.findOne({ titleId, isOriginalTitle: "1" })
        console.log("findAKAS", findAKAS);
        if (findAKAS) {
            var main_language_code = findAKAS.language
            var main_country_code = findAKAS.region
            if (main_language_code != "" || main_language_code != "\\N" || main_language_code != "\N" || main_country_code != "" || main_country_code != "\\N" || main_country_code != "\N") {
                Language.findOne({ code: main_language_code }, async (err, findLang) => {
                    if (findLang) {
                        main_language = findLang.name
                    }
                    console.log("main_language", main_language);
                    Country.findOne({ code: main_country_code }, async (err, findCountry) => {
                        if (findCountry) {
                            main_country = findCountry.name
                        }
                        console.log(main_country, main_language);
                        var update_merge = await Merge.updateOne({ tconst: titleId }, { main_country, main_language })
                        callback()
                    })
                })
            } else {
                var update_merge = await Merge.updateOne({ tconst: titleId }, { main_country, main_language })
            }
        }
        callback()
    }
})

router.get("/update_name", async (req, res) => {
    console.log("START");
    var page = Number(req.query.page)
    var limit = 10
    var skip = Number(limit * (page - 1))
    var all_merge = await Merge.find({}, { tconst: 1 }).limit(limit).skip(skip)
    k = 0
    loopArray()
    function loopArray() {
        if (k < all_merge.length) {
            bringData(() => {
                k++;
                loopArray()
            })
        } else {
            console.log("DONE");
            res.send({ status: 1, message: 'Done' })
        }
    }
    async function bringData(callback) {
        var tconst = all_merge[k].tconst
        console.log("tconst", tconst);
        var findTitle = await Title.findOne({ tconst }, { originalTitle: 1, primaryTitle: 1 })
        console.log("findTitle", findTitle);
        var update_title = await Merge.updateOne({ tconst }, { primaryTitle: findTitle.primaryTitle })
        console.log("update_title", update_title);
        callback()
    }
})

module.exports = router