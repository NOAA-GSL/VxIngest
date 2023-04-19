"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.App = exports.LogLevel = void 0;
var LogLevel;
(function (LogLevel) {
    LogLevel[LogLevel["DEBUG"] = 0] = "DEBUG";
    LogLevel[LogLevel["INFO"] = 1] = "INFO";
    LogLevel[LogLevel["WARN"] = 2] = "WARN";
    LogLevel[LogLevel["ERROR"] = 3] = "ERROR";
})(LogLevel || (LogLevel = {}));
exports.LogLevel = LogLevel;
class App {
    static log(loglevel, text) {
        if (undefined == loglevel || undefined == text) {
            return;
        }
        if (loglevel >= this.LOG_LEVEL) {
            switch (loglevel) {
                case LogLevel.DEBUG:
                    console.log("DEBUG\t[" + App.timeStamp() + "]\t" + text);
                    break;
                case LogLevel.INFO:
                    console.info("INFO\t[" + App.timeStamp() + "]\t" + text);
                    break;
                case LogLevel.WARN:
                    console.warn("WARN\t[" + App.timeStamp() + "]\t" + text);
                    break;
                case LogLevel.ERROR:
                    console.error("ERROR\t[" + App.timeStamp() + "]\t" + text);
                    break;
            }
        }
    }
    static timeStamp() {
        var d = new Date();
        return (d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds()
            + "." + d.getMilliseconds());
    }
    static elapsed_time(note) {
        var precision = 3; // 3 decimal places
        var elapsed = process.hrtime(this.start)[1] / 1000000; // divide by a million to get nano to milli
        console.log(process.hrtime(this.start)[0] + " s, " + elapsed.toFixed(precision) + " ms - " + note); // print message + time
        this.start = process.hrtime(); // reset the timer
    }
}
exports.App = App;
App.LOG_LEVEL = LogLevel.DEBUG;
App.start = process.hrtime();
