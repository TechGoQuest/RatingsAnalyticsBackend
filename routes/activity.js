const express = require("express");
const router = express.Router();
// var shell = require("shelljs");
var fs = require("fs");
const zlib = require('zlib');
var http = require("https")
const { spawn } = require('child_process');
const config = require("config")
var filepath = config.get("path").filepath
var main_path = config.get("path").main_path
var moment = require("moment");
var cron = require('node-cron');
const request = require("request")
const cheerio = require("cheerio")
const { send_mail, notEmpty, isNumeric, isEmptyArray, isNumberBetween, isFloatBetween, checkLanguage, checkCountry } = require("../common");
const { DATABASE_NAME, LIMIT_FOR_MERGE, MARGIN_OF_1_LIMIT, MARGIN_OF_1_OFFSET, RATING_MARGIN_VALUE, ABOVE_500_VOTES_LIMIT, ABOVE_500_VOTES_OFFSET, DIFFERENCE_OF_VOTES, MAIN_LANGUAGE_LIMIT, MAIN_COUNTRY_LIMIT, MIN_YEAR_LIMIT, MIN_RATING_LIMIT, ALERTS_EMAIL_ID, CHECK_EMAIL_ID, EMAIL_ID_FOR_TESTING, ALERTS_EMAIL_SUBJECT, CHECK_EMAIL_SUBJECT, AKAS_TSV_FILE_NAME, TITLE_TSV_FILE_NAME, RATING_TSV_FILE_NAME, CSV_PATH, FIELDS_NEEDED_IN_CSV, STEP_1_SUBJECT, STEP_2_SUBJECT, STEP_3_SUBJECT, STEP_4_SUBJECT, STEP_5_SUBJECT, STEP_6_SUBJECT, STEP_7_SUBJECT, STEP_8_SUBJECT, STEP_9_SUBJECT, STEP_10_SUBJECT, STEP_11_SUBJECT, STEP_12_SUBJECT, STEP_13_SUBJECT, STEP_14_SUBJECT, STEP_15_SUBJECT, STEP_16_SUBJECT, STEP_17_SUBJECT, STEP_18_SUBJECT, STEP_19_SUBJECT, STEP_20_SUBJECT, STEP_21_SUBJECT, STEP_22_SUBJECT, STEP_23_SUBJECT } = require("../constants")
const Rating = require("../models/rating_schema");
const Title = require("../models/title_schema");
const AKAS = require("../models/akas_schema");
const Merge = require("../models/merge_schema");
const Cron_table = require("../models/cron_table_schema");
const Country = require("../models/country_schema");
const Language = require("../models/language_schema");
const Setting = require("../models/settings_schema");
const Margin_of_1 = require("../models/margin_of_1_schema")
const Above_500_votes = require("../models/above_500_votes_schema");
const UnderProduction = require("../models/underproduction")
const Failed_validations = require("../models/failed_validations_schema");
const Passed_validations = require("../models/passed_validations_schema");

// 1. create_cron_table to monitor all the below steps
async function create_cron_log() {
    console.log("***START PROCESS 1- CRON LOG TABLE***", moment(new Date()).format("LTS"))
    var new_get_settings_data = 1
    var date = new Date()
    var todays_date = date.getDate() + "-" + Number(date.getMonth() + 1) + "-" + date.getFullYear()
    var allCronTables = await Cron_table.find({ operation_date: todays_date })
    console.log("allCronTables", allCronTables)
    var query = {
        operation_date: todays_date, // process start datetime
        limit: LIMIT_FOR_MERGE, // chunk of data merged at once
        offset: 0, // points to which chunk of data has to be merged
        created_on: new Date(), // log table datatime

        is_delete_akas_start: 0, // flag if akas data deletion has started
        is_delete_akas_end: 0, // flag if akas data deletion has ended
        is_download_unzip_akas_start: 0, // flag if akas data download/unzip has started 
        is_download_unzip_akas_end: 0, // flag if akas data download/unzip has ended
        is_dumping_akas_start: 0, // flag if akas data dumping has started
        is_dumping_akas_end: 0, // flag if akas data dumping has ended

        is_delete_title_start: 0, // flag if title data deletion has started
        is_delete_title_end: 0, // flag if title data deletion has ended
        is_download_unzip_title_start: 0, // flag if title data download/unzip has started 
        is_download_unzip_title_end: 0, // flag if title data download/unzip has ended
        is_dumping_title_start: 0, // flag if title data dumping has started
        is_dumping_title_end: 0, // flag if title data dumping has ended

        is_delete_rating_start: 0, // flag if rating data deletion has started
        is_delete_rating_end: 0, // flag if rating data deletion has ended
        is_download_unzip_rating_start: 0, // flag if rating data download/unzip has started 
        is_download_unzip_rating_end: 0, // flag if rating data download/unzip has ended
        is_dumping_rating_start: 0, // flag if rating data dumping has started
        is_dumping_rating_end: 0, // flag if rating data dumping has ended

        is_merging_start: 0, // flag if merging started
        is_merging_end: 0, // flag if merging ended
        total_data_this_week: 0, // total number of merged data got this week 

        is_delete_margin_of_1_start: 0, // flag if margin of 1 rating data deletion has started 
        is_delete_margin_of_1_end: 0, // flag if margin of 1 rating data deletion has ended
        margin_of_1: 0, // total number of margin of 1 data this week 
        margin_of_1_start: 0, // flag if margin of 1 rating data collection has started
        margin_of_1_end: 0, // flag if margin of 1 rating data collection has ended
        margin_of_1_limit: MARGIN_OF_1_LIMIT, // chunk of margin of 1 data returned at once
        margin_of_1_offset: MARGIN_OF_1_OFFSET, // points to next set of chunk

        is_delete_above_500_votes_start: 0, // flag if above 500 votes rating data deletion has started 
        is_delete_above_500_votes_end: 0, // flag if above 500 votes rating data deletion has ended
        above_500_votes: 0, // total number of above 500 votes data this week 
        above_500_votes_start: 0, // flag if above 500 votes rating data collection has started
        above_500_votes_end: 0, // flag if above 500 votes rating data collection has ended
        above_500_votes_limit: ABOVE_500_VOTES_LIMIT, // chunk of above 500 votes data returned at once
        above_500_votes_offset: ABOVE_500_VOTES_OFFSET, // points to next set of chunk

        is_delete_under_production_start: 0, // flag if under production data deletion has started 
        is_delete_under_production_end: 0, // flag if under production data deletion has ended 
        under_production: 0, // total number of under production data this week

        main_language_limit: MAIN_LANGUAGE_LIMIT, // scraping chunks of languages
        main_country_limit: MAIN_COUNTRY_LIMIT, // scraping chunks of country

        is_mail_sent: 0, // flag to check if the mail was sent 
    }
    if (allCronTables <= 0) {
        var get_settings_data = await Setting.findOne({})
        console.log("get_settings_data", get_settings_data)
        if (get_settings_data.is_process_complete == 1) {
            var cron = await Cron_table.create(query)
            console.log("cron", cron)
            if (get_settings_data) { new_get_settings_data = Number(get_settings_data.week_number) + 1 }
            var setting_update = await Setting.updateOne({}, { start_date: new Date(), this_week_folder_name: todays_date, week_number: new_get_settings_data, is_process_complete: 0 })
            console.log("***END PROCESS 1- CRON LOG TABLE***", moment(new Date()).format("LTS"))
        }
        try {
            download_unzip_akas()
        } catch {
            var error_message = "There was an error in the method- download_unzip_akas. Please check."
            var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
            send_mail(EMAIL_ID_FOR_TESTING, body, STEP_2_SUBJECT, async (response) => {
                console.log("EMAIL WAS SENT FOR ERROR IN DOWNLOAD AND UNZIP AKAS FILE")
            })
        }
    }
}
// 2. download and unzip akas file from https://datasets.imdbws.com/
async function download_unzip_akas() {
    console.log("***START PROCESS 2- DOWNLOAD AND UNZIP AKAS FILE***", moment(new Date()).format("LTS"))
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var full_date = operation_date.replace(/-/g, "_");

    Cron_table.findOne(cron_query, async (err, cron) => {
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
            const request = require('request');
            request(url, function (error, response, body) {
                var data = body.split("<ul>")
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
                var insert_path = filepath + full_date + '/'

                if (!fs.existsSync(insert_path)) {
                    console.log("excel_file not exist");
                    // shell.mkdir("-p", insert_path);
                    fs.mkdirSync(insert_path, { recursive: true })
                }
                var path = insert_path + output
                var file = fs.createWriteStream(path, "utf8");
                var request = http.get(url).on('response', function (res) {
                    console.log('in cb');
                    res.on('data', function (chunk) {
                        file.write(chunk);
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
                        console.log("***END PROCESS 2- DOWNLOAD AND UNZIP AKAS FILE***", moment(new Date()).format("LTS"))
                        try {
                            download_unzip_title()
                        }
                        catch {
                            var error_message = "There was an error in the method- download_unzip_title. Please check."
                            var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                            send_mail(EMAIL_ID_FOR_TESTING, body, STEP_3_SUBJECT, async (response) => {
                                console.log("EMAIL WAS SENT FOR ERROR IN DOWNLOAD AND UNZIP TITLE FILE")
                            })
                        }
                    }).on('error', function (err) {
                        console.log(err.message);
                    });
                });
            })
        }
    })

}
// 3. download and unzip title file from https://datasets.imdbws.com/
async function download_unzip_title() {
    console.log("***START PROCESS 3- DOWNLOAD AND UNZIP TITLE FILE***", moment(new Date()).format("LTS"))
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var full_date = operation_date.replace(/-/g, "_");

    Cron_table.findOne(cron_query, async (err, cron) => {
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
            const request = require('request');
            request(url, function (error, response, body) {
                var data = body.split("<ul>")
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
                var insert_path = filepath + full_date + '/'

                if (!fs.existsSync(insert_path)) {
                    console.log("excel_file not exist");
                    // shell.mkdir("-p", insert_path);
                    fs.mkdirSync(insert_path, true);
                }
                var path = insert_path + output
                var file = fs.createWriteStream(path, "utf8");
                var request = http.get(url).on('response', function (res) {
                    console.log('in cb');
                    res.on('data', function (chunk) {
                        file.write(chunk);
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
                        console.log("***END PROCESS 3- DOWNLOAD AND UNZIP TITLE FILE***", moment(new Date()).format("LTS"))
                        try {
                            download_unzip_rating()
                        }
                        catch {
                            var error_message = "There was an error in the method- download_unzip_rating. Please check."
                            var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                            send_mail(EMAIL_ID_FOR_TESTING, body, STEP_4_SUBJECT, async (response) => {
                                console.log("EMAIL WAS SENT FOR ERROR IN DOWNLOAD AND UNZIP RATING FILE")
                            })
                        }
                    }).on('error', function (err) {
                        console.log(err.message);
                    });
                });
            })
        }
    })
}
// 4. download and unzip rating file from https://datasets.imdbws.com/
async function download_unzip_rating() {
    console.log("***START PROCESS 4- DOWNLOAD AND UNZIP RATING FILE***", moment(new Date()).format("LTS"))
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var full_date = operation_date.replace(/-/g, "_");

    Cron_table.findOne(cron_query, async (err, cron) => {
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
                var insert_path = filepath + full_date + '/'

                if (!fs.existsSync(insert_path)) {
                    console.log("excel_file not exist");
                    // shell.mkdir("-p", insert_path);
                    fs.mkdirSync(insert_path, true);
                }
                var path = insert_path + output
                var file = fs.createWriteStream(path, "utf8");
                var request = http.get(url).on('response', function (res) {
                    console.log('in cb');
                    res.on('data', function (chunk) {
                        file.write(chunk);
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
                        console.log("***END PROCESS 4- DOWNLOAD AND UNZIP RATING FILE***", moment(new Date()).format("LTS"))
                        try {
                            delete_akas_data()
                        }
                        catch {
                            var error_message = "There was an error in the method- delete_akas_data. Please check."
                            var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                            send_mail(EMAIL_ID_FOR_TESTING, body, STEP_5_SUBJECT, async (response) => {
                                console.log("EMAIL WAS SENT FOR ERROR IN DELETE AKAS DATA")
                            })
                        }
                    }).on('error', function (err) {
                        console.log(err.message);
                    });
                });
            })
        }
    })
}
// 5. delete previous week akas data
async function delete_akas_data() {
    console.log("***START PROCESS 5- DELETE AKAS DATA***", moment(new Date()).format("LTS"))
    var settings = await Setting.findOne({})
    var operation_date = settings.this_week_folder_name
    var cron_query = { operation_date }
    var cron_search = await Cron_table.findOne(cron_query)
    if (!cron_search) {
        // res.send({ 'status': 1, 'message': 'CRON TABLE NOT FOUND' }) 
        console.log("CRON TABLE NOT FOUND", moment(new Date()).format("LTS"));
    }
    var akas_count = await AKAS.countDocuments({})
    console.log("akas_count", akas_count);
    if (akas_count > 0) {
        if ((cron_search.is_delete_akas_start == 0 && cron_search.is_delete_akas_end == 0) || (cron_search.is_delete_akas_start == 1 && cron_search.is_delete_akas_end == 0)) {
            if (cron_search.is_delete_akas_start == 0) {
                var update_cron_with = { is_delete_akas_start: 1, is_delete_akas_start_datetime: new Date() }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
            var delete_akas = await AKAS.deleteMany({})
            var akas_count = await AKAS.countDocuments({})
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
    // res.send({ 'status': 1 })
    // console.log("DONE-DELETE-AKAS", moment(new Date()).format("LTS"))
    console.log("***END PROCESS 5- DELETE AKAS DATA***", moment(new Date()).format("LTS"))
    try {
        delete_title_data()
    }
    catch {
        var error_message = "There was an error in the method- delete_title_data. Please check."
        var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
        send_mail(EMAIL_ID_FOR_TESTING, body, STEP_6_SUBJECT, async (response) => {
            console.log("EMAIL WAS SENT FOR ERROR IN DELETE TITLE DATA")
        })
    }
}
// 6. delete previous week title data
async function delete_title_data() {
    console.log("***START PROCESS 6- DELETE TITLE DATA***", moment(new Date()).format("LTS"))
    var settings = await Setting.findOne({})
    var operation_date = settings.this_week_folder_name
    var cron_query = { operation_date }
    var cron_search = await Cron_table.findOne(cron_query)
    if (!cron_search) {
        // res.send({ 'status': 1, 'message': 'CRON TABLE NOT FOUND' }) 
        console.log("CRON TABLE NOT FOUND", moment(new Date()).format("LTS"))
    }
    var title_count = await Title.countDocuments({})
    if (title_count > 0) {
        //continue deleting
        if ((cron_search.is_delete_title_start == 0 && cron_search.is_delete_title_end == 0) || (cron_search.is_delete_title_start == 1 && cron_search.is_delete_title_end == 0)) {
            if (cron_search.is_delete_title_start == 0) {
                var update_cron_with = { is_delete_title_start: 1, is_delete_title_start_datetime: new Date() }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
            var delete_title = await Title.deleteMany({})
            var title_count = await Title.countDocuments({})
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
    // res.send({ 'status': 1 })
    // console.log("DELETE TITLE", moment(new Date()).format("LTS"))
    console.log("***END PROCESS 6- DELETE TITLE DATA***", moment(new Date()).format("LTS"))
    try {
        delete_rating_data()
    }
    catch {
        var error_message = "There was an error in the method- delete_rating_data. Please check."
        var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
        send_mail(EMAIL_ID_FOR_TESTING, body, STEP_7_SUBJECT, async (response) => {
            console.log("EMAIL WAS SENT FOR ERROR IN DELETE RATING DATA")
        })
    }
}
// 7. delete previous week rating data
async function delete_rating_data() {
    console.log("***START PROCESS 7- DELETE RATING DATA***", moment(new Date()).format("LTS"))
    var settings = await Setting.findOne({})
    var operation_date = settings.this_week_folder_name
    var cron_query = { operation_date }
    var cron_search = await Cron_table.findOne(cron_query)
    if (!cron_search) {
        // res.send({ 'status': 1, 'message': 'CRON TABLE NOT FOUND' }) 
        console.log("CRON TABLE NOT FOUND", moment(new Date()).format("LTS"))
    }
    var rating_count = await Rating.countDocuments({})
    if (rating_count > 0) {
        //continue deleting
        if ((cron_search.is_delete_rating_start == 0 && cron_search.is_delete_rating_end == 0) || (cron_search.is_delete_rating_start == 1 && cron_search.is_delete_rating_end == 0)) {
            if (cron_search.is_delete_rating_start == 0) {
                var update_cron_with = { is_delete_rating_start: 1, is_delete_rating_start_datetime: new Date() }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
            var delete_rating = await Rating.deleteMany({})
            var rating_count = await Rating.countDocuments({})
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
    // res.send({ 'status': 1 })
    // console.log("DELETE RATING", moment(new Date()).format("LTS"))
    try {
        console.log("***END PROCESS 7- DELETE RATING DATA***", moment(new Date()).format("LTS"))
        import_akas_data()
    }
    catch {
        var error_message = "There was an error in the method- import_akas_data. Please check."
        var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
        send_mail(EMAIL_ID_FOR_TESTING, body, STEP_8_SUBJECT, async (response) => {
            console.log("EMAIL WAS SENT FOR ERROR IN IMPORTING AKAS DATA")
        })
    }
}
// 8. import present week akas data
async function import_akas_data() {
    console.log("***START PROCESS 8- IMPORT AKAS DATA***", moment(new Date()).format("LTS"))
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    console.log("setting", setting);
    Cron_table.findOne(cron_query, async (err, cron) => {
        if (cron.is_download_unzip_akas_end == 1) {
            var full_date = operation_date.replace(/-/g, "_");
            var fromTSVPath = filepath + full_date + AKAS_TSV_FILE_NAME

            // mongo import command to dump data from akas tsv file with more than 25+ million rows
            // as of sept 6th, 2021- 28999965 rows in akas file

            var sedCommandtoRead = ['--db', DATABASE_NAME, '--collection', 'akas', '--type', 'tsv', fromTSVPath, '--headerline']

            Cron_table.updateOne(cron_query, { is_dumping_akas_start: 1, is_dumping_akas_start_datetime: new Date() }, (err, update_start) => {

                const ls = spawn('mongoimport', sedCommandtoRead)
                ls.stdout.on('data', (data) => {
                    console.log(`stdout: ${data}`);
                });

                ls.stderr.on('data', (data) => {
                    console.error(`stderr: ${data}`);
                });

                ls.on('close', (code) => {
                    console.log(`child process exited with code ${code}`);
                    Cron_table.updateOne(cron_query, { is_dumping_akas_end: 1, is_dumping_akas_end_datetime: new Date() }, (err, update_end) => {
                        // res.send({ 'status': 1, 'message': 'AKAS file read successfully' })
                        console.log("AKAS file read successfully", moment(new Date()).format("LTS"))
                        try {
                            console.log("***END PROCESS 8- IMPORT AKAS DATA***", moment(new Date()).format("LTS"))
                            import_title_data()
                        }
                        catch {
                            var error_message = "There was an error in the method- import_title_data. Please check."
                            var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                            send_mail(EMAIL_ID_FOR_TESTING, body, STEP_9_SUBJECT, async (response) => {
                                console.log("EMAIL WAS SENT FOR ERROR IN IMPORTING TITLE DATA")
                            })
                        }
                    })
                });
            })
        } else {
            // res.send({ 'status': 1, 'message': 'Unzipping not finished yet' })
            console.log("Unzipping not finished yet", moment(new Date()).format("LTS"))
        }
    })
}
// 9. import present week title data
async function import_title_data() {
    console.log("***START PROCESS 9- IMPORT TITLE DATA***", moment(new Date()).format("LTS"))

    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    Cron_table.findOne(cron_query, async (err, cron) => {
        if (cron.is_download_unzip_title_end == 1) {
            var full_date = operation_date.replace(/-/g, "_");
            var fromTSVPath = filepath + full_date + TITLE_TSV_FILE_NAME

            // mongo import command to dump data from title tsv file with more than 8+ million rows
            // as of sept 6th, 2021- 8236801 rows in title file

            var sedCommandtoRead = ['--db', DATABASE_NAME, '--collection', 'titles', '--type', 'tsv', fromTSVPath, '--headerline']
            Cron_table.updateOne(cron_query, { is_dumping_title_start: 1, is_dumping_title_start_datetime: new Date() }, (err, update_start) => {
                const ls = spawn('mongoimport', sedCommandtoRead)
                ls.stdout.on('data', (data) => {
                    console.log(`stdout: ${data}`);
                });

                ls.stderr.on('data', (data) => {
                    console.error(`stderr: ${data}`);
                });

                ls.on('close', (code) => {
                    console.log(`child process exited with code ${code}`);
                    Cron_table.updateOne(cron_query, { is_dumping_title_end: 1, is_dumping_title_end_datetime: new Date() }, (err, update_end) => {
                        // res.send({ 'status': 1, 'message': 'Title file read successfully' })
                        console.log("Title file read successfully", moment(new Date()).format("LTS"))
                        try {
                            console.log("***END PROCESS 9- IMPORT TITLE DATA***", moment(new Date()).format("LTS"))
                            import_rating_data()
                        }
                        catch {
                            var error_message = "There was an error in the method- import_rating_data. Please check."
                            var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                            send_mail(EMAIL_ID_FOR_TESTING, body, STEP_10_SUBJECT, async (response) => {
                                console.log("EMAIL WAS SENT FOR ERROR IN IMPORTING RATING DATA")
                            })
                        }
                    })
                });
            })
        } else {
            // res.send({ 'status': 1, 'message': 'Unzipping not finished yet' })
            console.log("Unzipping not finished yet", moment(new Date()).format("LTS"))
        }
    })
}
// 10. import present week rating data
async function import_rating_data() {
    console.log("***START PROCESS 10- IMPORT RATING DATA***", moment(new Date()).format("LTS"))
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    Cron_table.findOne(cron_query, async (err, cron) => {
        if (cron.is_download_unzip_rating_end == 1) {
            var full_date = operation_date.replace(/-/g, "_");
            var fromTSVPath = filepath + full_date + RATING_TSV_FILE_NAME

            // mongo import command to dump data from rating tsv file with more than 1+ million rows
            // as of sept 6th, 2021- 1184397 rows in rating file

            var sedCommandtoRead = ['--db', DATABASE_NAME, '--collection', 'ratings', '--type', 'tsv', fromTSVPath, '--headerline']

            Cron_table.updateOne(cron_query, { is_dumping_rating_start: 1, is_dumping_rating_start_datetime: new Date() }, (err, update_start) => {
                const ls = spawn('mongoimport', sedCommandtoRead)
                ls.stdout.on('data', (data) => {
                    console.log(`stdout: ${data}`);
                });

                ls.stderr.on('data', (data) => {
                    console.error(`stderr: ${data}`);
                });

                ls.on('close', (code) => {
                    console.log(`child process exited with code ${code}`);
                    Cron_table.updateOne(cron_query, { is_dumping_rating_end: 1, is_dumping_rating_end_datetime: new Date() }, (err, update_end) => {
                        // res.send({ 'status': 1, 'message': 'Rating file read successfully' })
                        console.log("Rating file read successfully", moment(new Date()).format("LTS"))
                        try {
                            console.log("***END PROCESS 10- IMPORT RATING DATA***", moment(new Date()).format("LTS"))
                            merge_the_data()
                        }
                        catch {
                            var error_message = "There was an error in the method- merge_the_data. Please check."
                            var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                            send_mail(EMAIL_ID_FOR_TESTING, body, STEP_11_SUBJECT, async (response) => {
                                console.log("EMAIL WAS SENT FOR ERROR IN MERGING THE DATA")
                            })
                        }
                    })
                });
            })
        } else {
            // res.send({ 'status': 1, 'message': 'Unzipping not finished yet' })
            console.log("Unzipping not finished yet", moment(new Date()).format("LTS"))
        }
    })
}
// 11. merge the three tables
async function merge_the_data() {
    console.log("***START PROCESS 11- MERGING***", moment(new Date()).format("LTS"))
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
            console.log('start merge', offset, limit);
            console.log("start", moment(new Date()).format("LLLL"));

            merge()
        } else {
            try {
                console.log("***END PROCESS 11- MERGING***", moment(new Date()).format("LTS"))
                scrape_main_language()
            }
            catch {
                var error_message = "There was an error in the method- scrape_main_language. Please check."
                var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                send_mail(EMAIL_ID_FOR_TESTING, body, STEP_12_SUBJECT, async (response) => {
                    console.log("EMAIL WAS SENT FOR ERROR IN SCRAPING MAIN LANGUAGE")
                })
            }
        }
    }
    async function merge() {
        var query = [
            {
                $match: {
                    $and: [{ startYear: { $gte: MIN_YEAR_LIMIT } }, { startYear: { $ne: "\\N" } },
                    { $or: [{ "titleType": "tvMiniSeries" }, { "titleType": "tvSeries" }] }]
                }
            },
            { $lookup: { from: 'ratings', localField: "tconst", foreignField: "tconst", as: "rate" } },
            { $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "region" } },
            { $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "title" } },
            { $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "language" } },
            { $project: { startYear: 1, tconst: 1, titleType: 1, averageRating: "$rate.averageRating", numVotes: "$rate.numVotes", genres: 1, region: "$region.region", primaryTitle: 1, originalTitle: 1, language: "$language.language", alternateTitle: "$title.title" } },
            { $match: { $and: [{ averageRating: { $gte: MIN_RATING_LIMIT } }] } },
            { $skip: offset },
            { $limit: limit },
        ]
        Title.aggregate(query, async (err, data) => {
            if (cron_table.total_data_this_week == 0) {
                var query = [
                    {
                        $match: {
                            $and: [{ startYear: { $gte: MIN_YEAR_LIMIT } }, { startYear: { $ne: "\\N" } },
                            { $or: [{ "titleType": "tvMiniSeries" }, { "titleType": "tvSeries" }] }]
                        }
                    }, {
                        $lookup: { from: 'ratings', localField: "tconst", foreignField: "tconst", as: "rate" }
                    }, {
                        $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "region" }
                    }, {
                        $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "language" }
                    }, {
                        $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "title" }
                    }, {
                        $project: { startYear: 1, tconst: 1, titleType: 1, averageRating: "$rate.averageRating", numVotes: "$rate.numVotes", genres: 1, region: "$region.region", primaryTitle: 1, originalTitle: 1, language: "$language.language", alternateTitle: "$title.title" }
                    },
                    { $match: { $and: [{ averageRating: { $gte: MIN_RATING_LIMIT } }] } },
                ]
                var countMerges = await Title.aggregate(query)
                console.log('countMerges.length', countMerges.length);
                var update_cron_query = { total_data_this_week: countMerges.length }
                var update_cron = await Cron_table.updateOne(cron_query, update_cron_query)
            }
            console.log('merge data length', data.length);
            console.log('offset', offset);
            console.log('cron_table.total_data_this_week', cron_table.total_data_this_week);
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
                        console.log('merge if condition');
                        console.log('merge_data.length > 0', (merge_data.length > 0));
                        if (merge_data.length > 0) {
                            var merge_create = await Merge.insertMany(merge_data)
                        }
                        console.log('offset > cron_table.total_data_this_week', (offset > cron_table.total_data_this_week));

                        if (offset > cron_table.total_data_this_week) {
                            //end
                            console.log('update data after total length done');
                            var update_query = { is_merging_end: 1, is_merging_end_datetime: new Date() }
                            var update_cron = await Cron_table.updateOne(cron_query, update_query)
                            // *******
                            var update_setting = await Setting.updateOne({ _id: setting._id }, { cron_id: cron_table._id })
                            console.log('setting update', update_setting);
                            // *******
                            try {
                                console.log("***END PROCESS 11- MERGING***", moment(new Date()).format("LTS"))
                                scrape_main_language()
                            }
                            catch {
                                var error_message = "There was an error in the method- scrape_main_language. Please check."
                                var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                                send_mail(EMAIL_ID_FOR_TESTING, body, STEP_12_SUBJECT, async (response) => {
                                    console.log("EMAIL WAS SENT FOR ERROR IN SCRAPING MAIN LANGUAGE")
                                })
                            }
                        } else {
                            var newOffset = Number(offset) + LIMIT_FOR_MERGE
                            var update_cron = await Cron_table.updateOne(cron_query, { offset: newOffset })
                            try {
                                merge_the_data()
                            }
                            catch {
                                var error_message = "There was an error in the method- merge_the_data. Please check."
                                var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                                send_mail(EMAIL_ID_FOR_TESTING, body, STEP_11_SUBJECT, async (response) => {
                                    console.log("EMAIL WAS SENT FOR ERROR IN MERGING THE DATA")
                                })
                            }
                        }
                        // res.send({ status: 1, message: 'Done merging' })
                        console.log("Done merging", moment(new Date()).format("LTS"))
                    }
                }
                async function bringData(callback) {
                    var numVotes = []
                    var averageRating = []
                    var updateMerge = await Merge.updateOne({ tconst: titleID[k] }, { averageRating: [''], numVotes: [''] })
                    Merge.findOne({ tconst: titleID[k] }, async (err, merge) => {
                        //console.log("titleID[k]- there merge", titleID[k]);

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
                        console.log("k", k);
                        callback()
                    })
                }
            } else {
                if (cron_table.is_merging_end == 0 || !cron_table.is_merging_end || cron_table.is_merging_end == "" || cron_table.is_merging_end == "0") {
                    Cron_table.updateOne(cron_query, { is_merging_end: 1, is_merging_end_datetime: new Date() }, (err, up_cron) => {
                        console.log("Done merging", moment(new Date()).format("LTS"))
                    })
                    var update_setting = await Setting.updateOne({ _id: setting._id }, { cron_id: cron_table._id })
                    console.log('setting update data not found with offset', update_setting);
                } else {
                    // res.send({ 'status': 1, message: 'Done' })
                    console.log("Done merging", moment(new Date()).format("LTS"))
                }
                try {
                    scrape_main_language()
                }
                catch {
                    var error_message = "There was an error in the method- scrape_main_language. Please check."
                    var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                    send_mail(EMAIL_ID_FOR_TESTING, body, STEP_12_SUBJECT, async (response) => {
                        console.log("EMAIL WAS SENT FOR ERROR IN SCRAPING MAIN LANGUAGE")
                    })
                }
            }
        })
    }
}
// 12. scrape main language
async function scrape_main_language() {
    console.log("***START PROCESS 12- SCRAPING MAIN LANGUAGE***", moment(new Date()).format("LTS"))
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }

    var cron_table = await Cron_table.findOne(cron_query)

    // var limit = cron_table.main_country_limit
    // var skip = 0

    // var merge = await Merge.find({ main_language: { $eq: "" } }, { tconst: 1 }).limit(limit).skip(skip)
    var merge = await Merge.find({ main_language: { $eq: "" } }, { tconst: 1 })

    var count = merge.length

    k = 0
    loopArray()
    function loopArray() {
        if (k < merge.length) {
            bringData(() => {
                k++;
                loopArray()
            })
        } else {
            console.log("END", moment(new Date()).format("LTS"));
            console.log("***************DONE***************");
            try {
                console.log("***END PROCESS 12- SCRAPING MAIN LANGUAGE***", moment(new Date()).format("LTS"))
                scrape_main_country()
            }
            catch {
                var error_message = "There was an error in the method- scrape_main_country. Please check."
                var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                send_mail(EMAIL_ID_FOR_TESTING, body, STEP_13_SUBJECT, async (response) => {
                    console.log("EMAIL WAS SENT FOR ERROR IN SCRAPING MAIN COUNTRY")
                })
            }
        }
    }
    function bringData(callback) {
        var tconst = merge[k].tconst
        var url = "https://www.imdb.com/title/" + tconst + "/"
        var final_c = []
        var final_l = []
        var arr_c = []
        var arr_l = []
        var main_country = ""
        var main_language = ""

        request({ url }, async function (err, response, body) {
            const $ = cheerio.load(body);
            const title = $("title").text()
            var hrefInThisLink = $("a")
            hrefInThisLink.map((i, element) => {
                var url = element.attribs.href
                if (url != null) {
                    if (url.search("country_of_origin") != -1) {
                        if (url.startsWith("/")) {
                            url = "https://www.imdb.com" + url
                        }
                        var get_url = new URL(url)
                        var c = get_url.searchParams.get("country_of_origin")
                        final_c.push(c.toUpperCase())
                    }
                    if (url.search("primary_language") != -1) {
                        if (url.startsWith("/")) {
                            url = "https://www.imdb.com" + url
                        }
                        var get_url = new URL(url)
                        var l = get_url.searchParams.get("primary_language")
                        final_l.push(l)
                    }
                }
            });
            var coun = await Country.find({ code: { $in: final_c } }, { name: 1 })
            var lang = await Language.find({ code: { $in: final_l } }, { name: 1 })
            lang.forEach(element => {
                arr_l.push(element.name)
            });
            coun.forEach(element => {
                arr_c.push(element.name)
            });
            main_country = arr_c.join().replace(/,/g, ", ")
            main_language = arr_l.join().replace(/,/g, ", ")

            var update_merge = await Merge.updateOne({ tconst }, { main_country, main_language })
            console.log(k, "/", count, tconst, moment(new Date()).format("LTS"));
            callback()
        })
    }
}
// 13. scrape main country
async function scrape_main_country() {
    console.log("***START PROCESS 13- SCRAPING MAIN COUNTRY***", moment(new Date()).format("LTS"))

    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }

    var cron_table = await Cron_table.findOne(cron_query)

    // var limit = cron_table.main_country_limit
    // var skip = 0

    console.log("START", moment(new Date()).format("LTS"));
    // var merge = await Merge.find({ main_country: { $eq: "" } }, { tconst: 1 }).limit(limit).skip(skip) // 623
    var merge = await Merge.find({ main_country: { $eq: "" } }, { tconst: 1 })
    var count = merge.length

    k = 0
    loopArray()
    function loopArray() {
        if (k < merge.length) {
            bringData(() => {
                k++;
                loopArray()
            })
        } else {
            console.log("END", moment(new Date()).format("LTS"));
            console.log("***************DONE***************");
            // res.send({ status: 1 })
            try {
                console.log("***END PROCESS 13- SCRAPING MAIN COUNTRY***", moment(new Date()).format("LTS"))
                clear_margin_of_1_data()
            }
            catch {
                var error_message = "There was an error in the method- clear_margin_of_1_data. Please check."
                var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                send_mail(EMAIL_ID_FOR_TESTING, body, STEP_14_SUBJECT, async (response) => {
                    console.log("EMAIL WAS SENT FOR ERROR IN DELETING MARGIN OF 1 DATA OF LAST WEEK")
                })
            }
        }
    }
    function bringData(callback) {
        var tconst = merge[k].tconst
        var url = "https://www.imdb.com/title/" + tconst + "/"
        var final_c = []
        var final_l = []
        var arr_c = []
        var arr_l = []
        var main_country = ""
        var main_language = ""

        request({ url }, async function (err, response, body) {
            const $ = cheerio.load(body);
            const title = $("title").text()
            var hrefInThisLink = $("a")
            hrefInThisLink.map((i, element) => {
                var url = element.attribs.href
                if (url != null) {
                    if (url.search("country_of_origin") != -1) {
                        if (url.startsWith("/")) {
                            url = "https://www.imdb.com" + url
                        }
                        var get_url = new URL(url)
                        var c = get_url.searchParams.get("country_of_origin")
                        final_c.push(c.toUpperCase())
                    }
                    if (url.search("primary_language") != -1) {
                        if (url.startsWith("/")) {
                            url = "https://www.imdb.com" + url
                        }
                        var get_url = new URL(url)
                        var l = get_url.searchParams.get("primary_language")
                        final_l.push(l)
                    }
                }
            });
            var coun = await Country.find({ code: { $in: final_c } }, { name: 1 })
            var lang = await Language.find({ code: { $in: final_l } }, { name: 1 })
            lang.forEach(element => {
                arr_l.push(element.name)
            });
            coun.forEach(element => {
                arr_c.push(element.name)
            });
            main_country = arr_c.join().replace(/,/g, ", ")
            main_language = arr_l.join().replace(/,/g, ", ")

            var update_merge = await Merge.updateOne({ tconst }, { main_country, main_language })
            console.log(k, "/", count, tconst, moment(new Date()).format("LTS"));
            callback()
        })
    }
}
// 14. clear last weeks margin of 1 data
async function clear_margin_of_1_data() {
    console.log("***START PROCESS 14- DELETING MARGIN OF 1 DATA***", moment(new Date()).format("LTS"))

    var settings = await Setting.findOne({})
    var operation_date = settings.this_week_folder_name
    var cron_query = { operation_date }
    var cron_search = await Cron_table.findOne(cron_query)
    if (!cron_search) {
        // res.send({ 'status': 1, 'message': 'CRON TABLE NOT FOUND' }) 
        console.log("CRON TABLE NOT FOUND", moment(new Date()).format("LTS"))
    }
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
    // res.send({ 'status': 1 })
    console.log("DONE", moment(new Date()).format("LTS"))

    try {
        console.log("***END PROCESS 14- DELETING MARGIN OF 1 DATA***", moment(new Date()).format("LTS"))
        clear_above_500_votes_data()
    }
    catch {
        var error_message = "There was an error in the method- clear_above_500_votes_data. Please check."
        var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
        send_mail(EMAIL_ID_FOR_TESTING, body, STEP_15_SUBJECT, async (response) => {
            console.log("EMAIL WAS SENT FOR ERROR IN DELETING ABOVE 500 VOTES OF LAST WEEK")
        })
    }
}
// 15. clear last weeks above 500 votes data
async function clear_above_500_votes_data() {
    console.log("***START PROCESS 15- DELETING ABOVE 500 VOTES DATA***", moment(new Date()).format("LTS"))
    var settings = await Setting.findOne({})
    var operation_date = settings.this_week_folder_name
    var cron_query = { operation_date }
    var cron_search = await Cron_table.findOne(cron_query)
    if (!cron_search) {
        // res.send({ 'status': 1, 'message': 'CRON TABLE NOT FOUND' })
        console.log("CRON TABLE NOT FOUND", moment(new Date()).format("LTS"))
    }
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
    // res.send({ 'status': 1 })
    console.log("DONE", moment(new Date()).format("LTS"))

    try {
        console.log("***END PROCESS 15- DELETING ABOVE 500 VOTES DATA***", moment(new Date()).format("LTS"))
        clear_under_production_data()
    }
    catch {
        var error_message = "There was an error in the method- clear_under_production_data. Please check."
        var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
        send_mail(EMAIL_ID_FOR_TESTING, body, STEP_16_SUBJECT, async (response) => {
            console.log("EMAIL WAS SENT FOR ERROR IN DELETING UNDER PRODUCTION DATA OF LAST WEEK")
        })
    }
}
// 16. under production table
async function clear_under_production_data() {
    console.log("***START PROCESS 16- DELETING UNDER PRODUCTION DATA***", moment(new Date()).format("LTS"))

    var settings = await Setting.findOne({})
    var operation_date = settings.this_week_folder_name
    var cron_query = { operation_date }
    var cron_search = await Cron_table.findOne(cron_query)
    if (!cron_search) {
        // res.send({ 'status': 1, 'message': 'CRON TABLE NOT FOUND' }) 
        console.log("CRON TABLE NOT FOUND", moment(new Date()).format("LTS"))
    }
    var under_production_count = await UnderProduction.countDocuments({})
    if (under_production_count > 0) {
        //continue deleting
        if ((cron_search.is_delete_under_production_start == 0 && cron_search.is_delete_under_production_end == 0) || (cron_search.is_delete_under_production_start == 1 && cron_search.is_delete_under_production_end == 0)) {
            if (cron_search.is_delete_under_production_start == 0) {
                var update_cron_with = { is_delete_under_production_start: 1, is_delete_under_production_start_datetime: new Date() }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
            var delete_under_production = await UnderProduction.deleteMany({})
            if (under_production_count <= 0) {
                var update_cron_with = {
                    is_delete_under_production_end: 1,
                    is_delete_under_production_end_datetime: new Date()
                }
                var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
            }
        }
    } else {
        var update_cron_with = {
            is_delete_under_production_start: 1, is_delete_under_production_start_datetime: new Date(),
            is_delete_under_production_end: 1,
            is_delete_under_production_end_datetime: new Date()
        }
        var update_cron_table = await Cron_table.updateOne(cron_query, update_cron_with)
    }
    // res.send({ 'status': 1 })
    console.log("DONE", moment(new Date()).format("LTS"))

    try {
        console.log("***END PROCESS 16- DELETING UNDER PRODUCTION DATA***", moment(new Date()).format("LTS"))
        get_margin_of_1_data()
    }
    catch {
        var error_message = "There was an error in the method- get_margin_of_1_data. Please check."
        var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
        send_mail(EMAIL_ID_FOR_TESTING, body, STEP_17_SUBJECT, async (response) => {
            console.log("EMAIL WAS SENT FOR ERROR IN GETTING MARGIN OF 1 DATA OF THIS WEEK")
        })
    }
}
// 17. get margin of 1 data
async function get_margin_of_1_data() {
    console.log("***START PROCESS 17- GETTING MARGIN OF 1***", moment(new Date()).format("LTS"))

    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var cron_table = await Cron_table.findOne(cron_query)
    var limit = cron_table.margin_of_1_limit
    var skip = cron_table.margin_of_1_offset

    var total = cron_table.total_data_this_week

    if ((cron_table.margin_of_1_start == 0 && cron_table.margin_of_1_end == 0) || (cron_table.margin_of_1_start == 1 && cron_table.margin_of_1_end == 0)) {
        if (cron_table.margin_of_1_start == 0) {
            var update_query = { margin_of_1_start: 1, margin_of_1_start_datetime: new Date() }
            var update_cron = await Cron_table.updateOne(cron_query, update_query)
        }
        if (setting.week_number == 1) {
            var update_query = { margin_of_1: 0, margin_of_1_end: 1, margin_of_1_end_datetime: new Date() }
            var update_cron = await Cron_table.updateOne(cron_query, update_query)
            // res.send({ 'status': 1, 'message': 'Margin of 1 counting done' })
            console.log("Margin of 1 counting done", moment(new Date()).format("LTS"))
            try {
                console.log("***END PROCESS 17- GETTING MARGIN OF 1***", moment(new Date()).format("LTS"))
                get_above_500_votes_data()
            }
            catch {
                var error_message = "There was an error in the method- get_above_500_votes_data. Please check."
                var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                send_mail(EMAIL_ID_FOR_TESTING, body, STEP_18_SUBJECT, async (response) => {
                    console.log("EMAIL WAS SENT FOR ERROR IN GETTING ABOVE 500 VOTES DATA OF THIS WEEK")
                })
            }

        } else {
            var present = setting.week_number
            var past = Number(setting.week_number) - 1

            var rating_present = "week" + present + "_rating"
            var rating_past = "week" + past + "_rating"

            var titleIDs = []
            var ratings = []
            var votes = []
            var margin_of_1 = 0

            var allMerges = await Merge.find({}).limit(limit).skip(skip)

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
                    console.log('finished margin_of_1', margin_of_1);
                    // *******************
                    if (skip > total) {
                        //end
                        console.log('update data after total length done');
                        var merge_count = await Margin_of_1.countDocuments({})
                        var update_query = { margin_of_1: merge_count, margin_of_1_end: 1, margin_of_1_end_datetime: new Date() }
                        var update_cron = await Cron_table.updateOne(cron_query, update_query)

                        try {
                            console.log("***END PROCESS 17- GETTING MARGIN OF 1***", moment(new Date()).format("LTS"))
                            get_above_500_votes_data()
                        }
                        catch {
                            var error_message = "There was an error in the method- get_above_500_votes_data. Please check."
                            var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                            send_mail(EMAIL_ID_FOR_TESTING, body, STEP_18_SUBJECT, async (response) => {
                                console.log("EMAIL WAS SENT FOR ERROR IN GETTING ABOVE 500 VOTES DATA OF THIS WEEK")
                            })
                        }
                    } else {
                        var newOffset = Number(skip) + limit
                        var update_cron = await Cron_table.updateOne(cron_query, { margin_of_1_offset: newOffset })
                        try {
                            get_margin_of_1_data()
                        }
                        catch {
                            var error_message = "There was an error in the method- get_margin_of_1_data. Please check."
                            var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                            send_mail(EMAIL_ID_FOR_TESTING, body, STEP_17_SUBJECT, async (response) => {
                                console.log("EMAIL WAS SENT FOR ERROR IN GETTING MARGIN OF 1 DATA OF THIS WEEK")
                            })
                        }
                    }
                    // res.send({ 'status': 1, 'message': 'Margin of 1 counting done' })
                    console.log("Margin of 1 counting done", moment(new Date()).format("LTS"))

                    // *******************
                }
            }
            async function bringData(callback) {
                Merge.findOne({ tconst: titleIDs[k] }, async (err, merge) => {
                    if (merge) {
                        var newMerge = merge.toObject();
                        var present = merge[rating_present]
                        var past = merge[rating_past]
                        delete newMerge._id;
                        delete newMerge.__v;
                        console.log('k, titleID lenghth, titleIDs[k],present, past, margin_of_1', k, titleIDs.length, titleIDs[k], present, past, margin_of_1);
                        if (Number(present - past) > RATING_MARGIN_VALUE) {
                            margin_of_1++;
                            console.log('merge', newMerge);
                            var margin_create = await Margin_of_1.create(newMerge)
                        }
                    }
                    callback();
                })
            }
            // ****

        }
    }
}
// 18. get above 500 votes
async function get_above_500_votes_data() {
    console.log("***START PROCESS 18- GETTING ABOVE 500 VOTES***", moment(new Date()).format("LTS"))

    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var cron_table = await Cron_table.findOne(cron_query)

    var limit = cron_table.above_500_votes_limit
    var skip = cron_table.above_500_votes_offset

    var total = cron_table.total_data_this_week

    var present = setting.week_number
    if ((cron_table.above_500_votes_start == 0 && cron_table.above_500_votes_end == 0) || (cron_table.above_500_votes_start == 1 && cron_table.above_500_votes_end == 0)) {
        if (cron_table.above_500_votes_start == 0) {
            var update_query = { above_500_votes_start: 1, above_500_votes_start_datetime: new Date() }
            var update_cron = await Cron_table.updateOne(cron_query, update_query)
        }
        if (present == 1) {
            var update_query = { above_500_votes: 0, above_500_votes_end: 1, above_500_votes_end_datetime: new Date() }
            var update_cron = await Cron_table.updateOne(cron_query, update_query)
            // res.send({ 'status ': 1, 'message': 'Above 500 votes counting done' })
            console.log("Above 500 votes counting done", moment(new Date()).format("LTS"))
            try {
                console.log("***END PROCESS 18- GETTING ABOVE 500 VOTES***", moment(new Date()).format("LTS"))
                get_under_development_data()
            }
            catch {
                var error_message = "There was an error in the method- get_under_development_data. Please check."
                var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                send_mail(EMAIL_ID_FOR_TESTING, body, STEP_19_SUBJECT, async (response) => {
                    console.log("EMAIL WAS SENT FOR ERROR IN GETTING UNDER DEVELOPMENT DATA OF THIS WEEK")
                })
            }
        } else {
            var past = Number(setting.week_number) - 1

            var votes_present = "week" + present + "_votes"
            var rating_present = "week" + present + "_rating"

            var votes_past = "week" + past + "_votes"

            var titleIDs = []
            var votes = []
            var above_500_votes = 0
            var min_rating_limit = MIN_RATING_LIMIT.toString()
            var allMerges = await Merge.find({ [rating_present]: { $gte: min_rating_limit } }).limit(limit).skip(skip)

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
                    if (skip > total) {
                        //end
                        console.log('update data after total length done');
                        var above_500_votes = await Above_500_votes.countDocuments({})
                        var update_query = { above_500_votes, above_500_votes_end: 1, above_500_votes_end_datetime: new Date() }
                        var update_cron = await Cron_table.updateOne(cron_query, update_query)

                        try {
                            console.log("***END PROCESS 18- GETTING ABOVE 500 VOTES***", moment(new Date()).format("LTS"))
                            get_under_development_data()
                        }
                        catch {
                            var error_message = "There was an error in the method- get_under_development_data. Please check."
                            var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                            send_mail(EMAIL_ID_FOR_TESTING, body, STEP_19_SUBJECT, async (response) => {
                                console.log("EMAIL WAS SENT FOR ERROR IN GETTING UNDER DEVELOPMENT DATA OF THIS WEEK")
                            })
                        }
                    } else {
                        var newOffset = Number(skip) + limit
                        var update_cron = await Cron_table.updateOne(cron_query, { above_500_votes_offset: newOffset })
                        try {
                            get_above_500_votes_data()
                        }
                        catch {
                            var error_message = "There was an error in the method- get_above_500_votes_data. Please check."
                            var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                            send_mail(EMAIL_ID_FOR_TESTING, body, STEP_18_SUBJECT, async (response) => {
                                console.log("EMAIL WAS SENT FOR ERROR IN GETTING ABOVE 500 VOTES DATA OF THIS WEEK")
                            })
                        }
                    }
                    // res.send({ 'status ': 1, 'message': 'Avove 500 votes counting done' })
                    console.log("Above 500 votes counting done", moment(new Date()).format("LTS"))
                }
            }
            async function bringData(callback) {
                Merge.findOne({ tconst: titleIDs[k] }, async (err, merge) => {
                    console.log("gererre", titleIDs[k]);
                    if (merge) {
                        var newMerge = merge.toObject();
                        var present = merge[votes_present]
                        var past = merge[votes_past]
                        if (!past) {
                        } else {
                            delete newMerge._id;
                            delete newMerge.__v;
                            var findID = await Above_500_votes.find({ tconst: titleIDs[k] })
                            if (Number(present - past) >= DIFFERENCE_OF_VOTES && findID.length <= 0) {
                                console.log("newMerge", newMerge);
                                var create_merge = await Above_500_votes.create(newMerge)
                                above_500_votes++;
                            }
                        }
                    }
                    callback()
                })
            }
        }
    }
}
// 19. add under development projects
async function get_under_development_data() {
    console.log("***START PROCESS 19- GETTING UNDER DEVELOPMENT***", moment(new Date()).format("LTS"))

    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }

    var date = new Date()
    var year = Number(date.getFullYear() + 1) // next year
    var page = 1
    var limit = 500
    var skip = limit * Number(page - 1)
    var tConstArr = []
    var query = [
        {
            $match: {
                $and: [{ startYear: { $gte: year } }, { startYear: { $ne: "\\N" } },
                { $or: [{ "titleType": "tvMiniSeries" }, { "titleType": "tvSeries" }] }]
            }
        }, {
            $lookup: { from: 'akas', localField: "tconst", foreignField: "titleId", as: "akas" }
        }, {
            $project: { startYear: 1, tconst: 1, titleType: 1, genres: 1, region: "$akas.region", primaryTitle: 1, originalTitle: 1, language: "$akas.language" }
        },
        { $skip: skip },
        { $limit: limit }
    ]
    console.log("query", query);
    Title.aggregate(query, async (err, data) => {
        console.log("DATA-LENGTH", data.length);

        console.log("DATA", data);

        data.forEach(element => {
            tConstArr.push({ tconst: element.tconst, language: element.language, region: element.region })
        });
        var lang_arr = await Language.find({}, { __v: 0, _id: 0 })
        var coun_arr = await Country.find({}, { __v: 0, _id: 0 })
        k = 0
        loopArray()
        async function loopArray() {
            if (k < tConstArr.length) {
                bringData(() => {
                    k++;
                    loopArray()
                })
            } else {
                // res.send({ status: 1, data, count: data.length })
                console.log("DONE", moment(new Date()).format("LTS"))
                var under_production = await UnderProduction.countDocuments({})
                var un = { under_production }
                var update = await Cron_table.updateOne(cron_query, un)

                try {
                    console.log("***END PROCESS 19- GETTING UNDER DEVELOPMENT***", moment(new Date()).format("LTS"))
                    send_alerts_mail()
                }
                catch {
                    var error_message = "There was an error in the method- send_alerts_mail. Please check."
                    var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                    send_mail(EMAIL_ID_FOR_TESTING, body, STEP_20_SUBJECT, async (response) => {
                        console.log("EMAIL WAS SENT FOR ERROR IN SENDING ALERTS MAIL")
                    })
                }
            }
        }
        async function bringData(callback) {
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
            delete data[k]._id;
            var create_underproduction = await UnderProduction.create(data[k])
            callback()
        }
    })
}
// 20. send alerts mail
async function send_alerts_mail() {
    console.log("***START PROCESS 20- SENDING ALERTS MAIL***", moment(new Date()).format("LTS"))

    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var cron_table = await Cron_table.findOne(cron_query)
    var full_date = operation_date.replace(/-/g, "_");

    var present = setting.week_number
    var past = Number(setting.week_number) - 1

    var rating_present = "week" + present + "_rating"
    var rating_past = "week" + past + "_rating"

    var above_6 = await Merge.find({ $and: [{ [rating_past]: { $exists: false } }, { [rating_present]: { $exists: true } }] }).countDocuments({})

    var below_6 = await Merge.find({ [rating_present]: { $exists: false } }).countDocuments({})

    if (cron_table.margin_of_1_end == 1 && cron_table.above_500_votes_end == 1) {
        if (cron_table.is_mail_sent == 1) { // don't send
            // res.send({ 'status': 1, 'message': 'Email already sent' })
            console.log("Email already sent", moment(new Date()).format("LTS"))

            try {
                console.log("***END PROCESS 20- SENDING ALERTS MAIL***", moment(new Date()).format("LTS"))
                validations()
            }
            catch {
                var error_message = "There was an error in the method- validations. Please check."
                var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                send_mail(EMAIL_ID_FOR_TESTING, body, STEP_21_SUBJECT, async (response) => {
                    console.log("EMAIL WAS SENT FOR ERROR IN CHECKING VALIDATIONS")
                })
            }
        } else {
            var body =
                "<p>ALERTS REQUIRED BY AQUISITION TEAM</p><br><p>The following are the alerts raised for IMDb data analysis</p><br><ul><li><a href = 'http://139.59.18.134:5000/emaildata?id=1'>All Shows that have increased from Above 6 by a margin of 1 full point in ratings : " + cron_table.margin_of_1 + "</a></li><li><a href = 'http://139.59.18.134:5000/emaildata?id=2'> All Shows that have increased from Above 6 and an increase of atleast 500 votes: " + cron_table.above_500_votes + "</a></li><li><a href = 'http://139.59.18.134:5000/emaildata?id=3'>All Shows that have increased their rating from below 6 to above 6 : " + above_6 + "</a></li><li><a href = 'http://139.59.18.134:5000/emaildata?id=4'>All Shows that have decreased their rating from above 6 to below 6 : " + below_6 + "</a></li></ul>";
            send_mail(EMAIL_ID_FOR_TESTING, body, ALERTS_EMAIL_SUBJECT, async (response) => {
                var setting_update = await Setting.updateOne({}, { end_date: new Date(), is_process_complete: 1 });
                if (response) {
                    var send_email = await Cron_table.updateOne(cron_query, { is_mail_sent: 1, is_mail_sent_on: new Date() })
                    fs.unlinkSync(filepath + full_date + "/" + "title.akas.tsv");
                    fs.unlinkSync(filepath + full_date + "/" + "title.basics.tsv");
                    fs.unlinkSync(filepath + full_date + "/" + "title.ratings.tsv");
                    console.log("Email sent", moment(new Date()).format("LTS"))

                    try {
                        console.log("***END PROCESS 20- SENDING ALERTS MAIL***", moment(new Date()).format("LTS"))
                        validations()
                    }
                    catch {
                        var error_message = "There was an error in the method- validations. Please check."
                        var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                        send_mail(EMAIL_ID_FOR_TESTING, body, STEP_21_SUBJECT, async (response) => {
                            console.log("EMAIL WAS SENT FOR ERROR IN CHECKING VALIDATIONS")
                        })
                    }
                }
            })
        }
    } else {
        console.log('data not set bedore mail');
        var setting_update = await Setting.updateOne({}, { end_date: new Date(), is_process_complete: 1 })
    }
}
// 21. validations
async function validations() {
    console.log("***START PROCESS 21- CHECKING VALIDATIONS***", moment(new Date()).format("LTS"))
    var merged_data = await Merge.find({})
    k = 0
    loopArray()
    function loopArray() {
        if (k < merged_data.length) {
            bringData(() => {
                k++;
                loopArray()
            })
        } else {
            console.log("DATA END", moment(new Date()).format("LTS"))
            console.log(`Validations done for ${merged_data.length} rows`)

            try {
                console.log("***END PROCESS 21- CHECKING VALIDATIONS***", moment(new Date()).format("LTS"))
                create_csv_for_passed_validations()
            }
            catch {
                var error_message = "There was an error in the method- create_csv_for_passed_validations. Please check."
                var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
                send_mail(EMAIL_ID_FOR_TESTING, body, STEP_22_SUBJECT, async (response) => {
                    console.log("EMAIL WAS SENT FOR ERROR IN CREATING THE CSV")
                })
            }
        }
    }
    async function bringData(callback) {
        console.log("DATA", k, moment(new Date()).format("LTS"))

        var arr = []
        var merge_single_data = merged_data[k]

        var newMerge = merge_single_data.toObject();
        delete newMerge._id;
        delete newMerge.__v;
        notEmpty(newMerge.primaryTitle.trim()) ? arr.push("Title") : null
        notEmpty(newMerge.tconst) ? arr.push("Title ID") : null
        notEmpty(newMerge.startYear) ? arr.push("Year") : null
        isNumeric(Number(newMerge.startYear)) ? arr.push("Year") : null
        isNumberBetween(Number(newMerge.startYear)) ? arr.push("Year") : null
        notEmpty(newMerge.averageRating) ? arr.push("Rating") : null
        isEmptyArray(newMerge.averageRating) ? arr.push("Rating") : null
        isFloatBetween(Number(newMerge.averageRating)) ? arr.push("Rating") : null
        notEmpty(newMerge.numVotes) ? arr.push("Votes") : null
        isEmptyArray(newMerge.numVotes) ? arr.push("Votes") : null
        isNumeric(Number(newMerge.numVotes)) ? arr.push("Votes") : null
        notEmpty(newMerge.titleType) ? arr.push("Type") : null
        isEmptyArray(newMerge.genres) ? arr.push("Genres") : null
        notEmpty(newMerge.main_language) ? arr.push("Language") : null
        checkLanguage(newMerge.main_language) ? arr.push("Language") : null
        notEmpty(newMerge.main_country) ? arr.push("Region") : null
        checkCountry(newMerge.main_country) ? arr.push("Region") : null

        var findInFailed = await Failed_validations.find({ tconst: newMerge.tconst })
        var findInPassed = await Passed_validations.find({ tconst: newMerge.tconst })
        if (arr.length > 0) {
            newMerge.issues_in = arr
            findInFailed.length > 0 ? await Failed_validations.updateOne({ tconst: newMerge.tconst }, { issues_in: arr }) : await Failed_validations.create(newMerge)
            findInPassed.length > 0 ? await Passed_validations.deleteOne({ tconst: newMerge.tconst }) : null
        } else {
            findInPassed.length > 0 ? null : await Passed_validations.create(newMerge)
            findInFailed.length > 0 ? await Failed_validations.deleteOne({ tconst: newMerge.tconst }) : null
        }
        callback()
    }
}
// 22. create csv for passed validations
async function create_csv_for_passed_validations() {
    console.log("***START PROCESS 22- CREATE PASSED VALIDATIONS CSV***", moment(new Date()).format("LTS"))
    var fromCSVPath = main_path + CSV_PATH
    var sedCommandtoRead = ['--db', DATABASE_NAME, '--collection', 'passed_validations', '--type', 'csv', '--fields', FIELDS_NEEDED_IN_CSV, '--out', fromCSVPath]

    const ls = spawn('mongoexport', sedCommandtoRead)
    ls.stdout.on('data', (data) => {
        console.log(`stdout: ${data}`);
    });

    ls.stderr.on('data', (data) => {
        console.error(`stderr: ${data}`);
    });

    ls.on('close', (code) => {
        console.log(`child process exited with code ${code}`);
        console.log("Passed validations imported successfully", moment(new Date()).format("LTS"))
        try {
            console.log("***END PROCESS 22- CREATE PASSED VALIDATIONS CSV***", moment(new Date()).format("LTS"))
            check_mail_to_check_the_steps()
        }
        catch {
            var error_message = "There was an error in the method- check_mail_to_check_the_steps. Please check."
            var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
            send_mail(EMAIL_ID_FOR_TESTING, body, STEP_23_SUBJECT, async (response) => {
                console.log("EMAIL WAS SENT FOR ERROR IN SENDING CHECK MAIL")
            })
        }
    });
}
// 23. check mail to check the steps
async function check_mail_to_check_the_steps() {
    console.log("***START PROCESS 23- SEND CHECK MAIL***", moment(new Date()).format("LTS"))
    var setting = await Setting.findOne({})
    var operation_date = setting.this_week_folder_name
    var cron_query = { operation_date }
    var cron_table = await Cron_table.findOne(cron_query)
    var merge = await Merge.count()
    var passed = await Passed_validations.count()
    var failed = await Failed_validations.count()

    var full_date = operation_date.replace(/-/g, "_");
    var error = []
    var warning = []
    const { is_download_unzip_akas_start, is_download_unzip_akas_end, is_akas_convert, is_akas_split, is_dumping_akas_start, is_dumping_akas_end, is_download_unzip_title_start, is_download_unzip_title_end, is_title_convert, is_title_split, is_dumping_title_start, is_dumping_title_end, is_download_unzip_rating_start, is_download_unzip_rating_end, is_rating_convert, is_rating_split, is_dumping_rating_start, is_dumping_rating_end, margin_of_1, is_merging_start, is_merging_end, margin_of_1_start, margin_of_1_end, above_500_votes, above_500_votes_start, above_500_votes_end, above_6, above_6_start, above_6_end, below_6, below_6_start, below_6_end, is_mail_sent, under_production } = cron_table
    if (is_download_unzip_akas_start == 0) { error.push("AKAS downloading and unzipping did not start") }
    if (is_download_unzip_akas_end == 0) { error.push("AKAS downloading and unzipping did not end") }
    if (is_dumping_akas_start == 0) { error.push("AKAS data dumping did not start") }
    if (is_dumping_akas_end == 0) { error.push("AKAS data dumping did not end") }
    if (is_download_unzip_title_start == 0) { error.push("Title downloading and unzipping did not start") }
    if (is_download_unzip_title_end == 0) { error.push("AKAS downloading and unzipping did not end") }
    if (is_dumping_title_start == 0) { error.push("Title data dumping did not start") }
    if (is_dumping_title_end == 0) { error.push("Title data dumping did not end") }
    if (is_download_unzip_rating_start == 0) { error.push("Rating downloading and unzipping did not start") }
    if (is_download_unzip_rating_end == 0) { error.push("Rating downloading and unzipping did not end") }
    if (is_dumping_rating_start == 0) { error.push("Rating data dumping did not start") }
    if (is_dumping_rating_end == 0) { error.push("Rating data dumping did not end") }
    if (margin_of_1 == 0) { warning.push("Margin of 1 data is 0.") }
    if (is_merging_start == 0) { error.push("Merging of data did not start") }
    if (is_merging_end == 0) { error.push("Merging of data did not end") }
    if (margin_of_1_start == 0) { error.push("Margin of 1 data grouping did not start") }
    if (margin_of_1_end == 0) { error.push("Margin of 1 data grouping did not end") }
    if (above_500_votes == 0) { warning.push("Above 500 votes data is 0.") }
    if (under_production == 0) { warning.push("Under production data is 0.") }
    if (above_500_votes_start == 0) { error.push("Above 500 votes data grouping did not start") }
    if (above_500_votes_end == 0) { error.push("Above 500 votes data grouping did not end") }
    if (is_mail_sent == 0) { error.push("Email was not sent") }

    var process_status = (setting.is_process_complete == 0) ? "No" : "Yes"
    var error_message = (error.length > 0) ? "There were some errors in the system, this week." : "No error were found this week."
    var warning_message = (warning.length > 0) ? "There were some warnings in the system, this week." : "No warnings were raised this week."
    var error_list = ""
    if (error.length > 0) {
        error.forEach(element => {
            error_list += "<li>" + element + "</li>"
        });
    }
    var warning_list = ""
    if (warning.length > 0) {
        warning.forEach(element => {
            warning_list += "<li>" + element + "</li>"
        });
    }
    var observation_list = `<ul><li>Ratings Db process started at : ${moment(setting.start_date).format("LLLL")}</li><li>Total number of records imported : ${cron_table.total_data_this_week}</li><li>Total number of records already existing : ${merge}</li><li>Total number of records as per goquest criteria : ${passed}</li><li>Total number of records with failed validations : ${failed}</li><li>Total number of records ready for evaluation : ${passed}</li><li>Ratings Db process completed at : ${moment(setting.end_date).format("LLLL")}</li><li>Download the CSV file here : <a href='http://139.59.18.134:4008/1.csv'>CSV Link</a></li></ul>`

    var body = "<p>Hi</p>" + "<p>" + error_message + "</p>" + "<ul>" + error_list + "</ul></br>" + "<p>" + warning_message + "</p>" + "<ul>" + warning_list + "</ul></br>" + "<strong><p>DID THE PROCESS COMPLETE?: " + process_status + "</p></strong></br><strong><p>Observations as per new Requirement</p></br>" + observation_list + "</strong>";

    send_mail(EMAIL_ID_FOR_TESTING, body, CHECK_EMAIL_SUBJECT, async (response) => {
        console.log('PROCESSES DONE')
        console.log("***END PROCESS 23- SEND CHECK MAIL***", moment(new Date()).format("LTS"))
    })
}
// STARTING CRON
cron.schedule('30 11 * * 2', async () => {
    try {
        create_cron_log()
    }
    catch {
        var error_message = "There was an error in the method- create_cron_log. Please check."
        var body = "<p>Hi</p>" + "<p>" + error_message + "</p><p>Thank you.</p>"
        send_mail(EMAIL_ID_FOR_TESTING, body, STEP_1_SUBJECT, async (response) => {
            console.log("EMAIL WAS SENT FOR ERROR IN CREATE CRON LOG")
        })
    }
})
module.exports = router;