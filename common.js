const nodemailer = require("nodemailer");
const Language = require("./models/language_schema");
const Country = require("./models/country_schema");

var emailtest = {
    "fromEmail": "noreply@flickquickapp.com",
    "fromName": "FLICKQUICK",
    "hostname": "smtp.sparkpostmail.com",
    "userName": "SMTP_Injection",
    "apiKey": "bd99cb8ff7487d7c3e89befe064ee3a6e3a6d443"
}
var helper = {
    send_mail: function send_mail(emailIDs, body, subject, callback) {
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
        mailOptions = {
            from: emailtest.fromEmail,
            to: emailIDs,
            subject: subject,
            html: body
        };
        console.log("body", subject, body)
        callback(true)
        return
        transporter.sendMail(mailOptions, async function (error, info) {
            if (!error) {
                callback(true)
            } else {
                callback(false)
            }
        });
    },
    notEmpty: function notEmpty(data) {
        if (!data || data == "" || data == "//N") {
            return true
        } else {
            return false
        }
    },
    isNumeric: function isNumeric(data) {
        if (Number.isInteger(data)) {
            return false
        } else {
            return true
        }
    },
    isEmptyArray: function isEmptyArray(data) {
        if (data.length > 0) {
            return false
        } else {
            return true
        }
    },
    isNumberBetween: function isNumberBetween(data) {
        if (data >= 1000 && data <= 9999) {
            return false
        } else {
            return true
        }
    },
    isFloatBetween: function isFloatBetween(data) {
        if (data >= 0.0 && data <= 10.0) {
            return false
        } else {
            return true
        }
    },
    checkLanguage: function checkLanguage(data) {
        Language.find({ name: data }, (err, res) => {
            if (res.length > 0) {
                return false
            } else {
                return true
            }
        })
    },
    checkCountry: function checkCountry(data) {
        Country.find({ name: data }, (err, res) => {
            if (res.length > 0) {
                return false
            } else {
                return true
            }
        })
    }
}
module.exports = helper