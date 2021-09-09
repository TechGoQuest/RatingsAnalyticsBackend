var mongoose = require("mongoose");

var setting = new mongoose.Schema({ // always update this table
    week_number: Number,
    past_week_date: Date,
    present_week_date: Date,
    this_week_folder_name: String,
    is_process_complete: Number,
    cron_id: String,
    limit_for_validations: { type: Number, default: 1000 },
    offset_for_validations: Number,
    start_date: Date,
    end_date: Date,
});

var Setting = mongoose.model("Setting", setting);
module.exports = Setting;
