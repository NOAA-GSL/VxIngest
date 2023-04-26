'use strict';
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.CbRunQueries = void 0;
const fs_1 = __importDefault(require("fs"));
const App_1 = require("./App");
const couchbase_1 = require("couchbase");
class CbRunQueries {
    constructor() {
        this.configFile = "./config/config.json";
        this.config = JSON.parse(fs_1.default.readFileSync(this.configFile, 'utf-8'));
        this.settings = JSON.parse(fs_1.default.readFileSync(this.config.settingsFile, 'utf-8'));
        // public clusterConnStr: string = 'couchbase://localhost'
        this.clusterConnStr = 'adb-cb1.gsd.esrl.noaa.gov';
        this.bucketName = 'vxdata';
        this.cluster = null;
        this.bucket = null;
        this.collection = null;
    }
    init() {
        return __awaiter(this, void 0, void 0, function* () {
            // App.log(LogLevel.INFO, "settings:" + JSON.stringify(settings, undefined, 2));
            App_1.App.log(App_1.LogLevel.INFO, "user:" + this.settings.private.databases[0].user);
            this.cluster = yield (0, couchbase_1.connect)(this.clusterConnStr, {
                username: this.settings.private.databases[0].user,
                password: this.settings.private.databases[0].password,
                timeouts: {
                    kvTimeout: 3600000,
                    queryTimeout: 3600000
                }
            });
            this.bucket = this.cluster.bucket(this.bucketName);
            // Get a reference to the default collection, required only for older Couchbase server versions
            this.collection = this.bucket.scope('_default').collection('METAR');
            App_1.App.log(App_1.LogLevel.INFO, "Connected!");
        });
    }
    shutdown() {
        return __awaiter(this, void 0, void 0, function* () {
        });
    }
    runQueryFile(queryFile, writeOutput) {
        return __awaiter(this, void 0, void 0, function* () {
            App_1.App.log(App_1.LogLevel.INFO, "runQueryFile(" + queryFile + ")");
            let startTime = (new Date()).valueOf();
            var sqlstr = fs_1.default.readFileSync("./SQLs/" + queryFile, 'utf-8');
            const queryResult = yield this.bucket.scope('_default').query(sqlstr, {
                parameters: [],
            });
            if (true === writeOutput) {
                fs_1.default.writeFileSync('./output/queryResult.json', "");
                let fd = fs_1.default.openSync('./output/queryResult.json', 'a');
                let lines = 0;
                queryResult.rows.forEach((row) => {
                    fs_1.default.writeSync(fd, JSON.stringify(row, null, 2));
                    if ((++lines % 1000) === 0) {
                        console.log("\tcount:" + lines);
                    }
                });
                fs_1.default.closeSync(fd);
                console.log('Query Results (Obs):' + queryResult.rows.length + " written to file ./outputs/queryResult.json");
            }
            else {
                console.log('Query Results (Obs):' + queryResult.rows.length);
            }
            let endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\trows:" + queryResult.rows.length + " in " + (endTime - startTime) + " ms.");
        });
    }
    runObsModelQueries(obsSqlFile, modelSqlFile, writeOutput) {
        return __awaiter(this, void 0, void 0, function* () {
            App_1.App.log(App_1.LogLevel.INFO, "runObsModelQueries(" + obsSqlFile + "," + modelSqlFile + ")");
            let obs = {};
            let model = {};
            let startTime = (new Date()).valueOf();
            var sqlstrObs = fs_1.default.readFileSync("./SQLs/" + obsSqlFile, 'utf-8');
            const queryResultObs = yield this.bucket.scope('_default').query(sqlstrObs, {
                parameters: [],
            });
            if (true === writeOutput) {
                fs_1.default.writeFileSync('./output/queryResultObs.json', "");
                let fd = fs_1.default.openSync('./output/queryResultObs.json', 'a');
                let lines = 0;
                queryResultObs.rows.forEach((row) => {
                    fs_1.default.writeSync(fd, JSON.stringify(row, null, 2));
                    if ((++lines % 1000) === 0) {
                        console.log("\tcount:" + lines);
                    }
                });
                fs_1.default.closeSync(fd);
                console.log('Query Results (Obs):' + queryResultObs.rows.length + " written to file ./outputs/queryResultObs.json");
            }
            else {
                console.log('Query Results (Obs):' + queryResultObs.rows.length);
            }
            let endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tqueryResultObs rows:" + queryResultObs.rows.length + " in " + (endTime - startTime) + " ms.");
            var sqlstrModel = fs_1.default.readFileSync("./SQLs/" + modelSqlFile, 'utf-8');
            const queryResultModel = yield this.bucket.scope('_default').query(sqlstrModel, {
                parameters: [],
            });
            if (true === writeOutput) {
                fs_1.default.writeFileSync('./output/queryResultModel.json', "");
                let fd = fs_1.default.openSync('./output/queryResultModel.json', 'a');
                let lines = 0;
                queryResultModel.rows.forEach((row) => {
                    fs_1.default.writeSync(fd, JSON.stringify(row, null, 2));
                    if ((++lines % 1000) === 0) {
                        console.log("\tcount:" + lines);
                    }
                });
                fs_1.default.closeSync(fd);
                console.log('Query Results (Model):' + queryResultModel.rows.length + " written to file ./outputs/queryResultModel.json");
            }
            else {
                console.log('Query Results (Model):' + queryResultModel.rows.length);
            }
            endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tqueryResultModel rows:" + queryResultModel.rows.length + " in " + (endTime - startTime) + " ms.");
        });
    }
    runOrgStationQueryFinalSaveToFile() {
        return __awaiter(this, void 0, void 0, function* () {
            App_1.App.log(App_1.LogLevel.INFO, "runOrgStationQueryFinalSaveToFile()");
            let startTime = (new Date()).valueOf();
            var sqlstr = fs_1.default.readFileSync("./SQLs/final_TimeSeries.sql", 'utf-8');
            const queryResult = yield this.bucket.scope('_default').query(sqlstr, {
                parameters: [],
            });
            fs_1.default.writeFileSync('./output/runOrgStationQueryFinalSaveToFile.json', JSON.stringify(queryResult.rows, null, 2));
            console.log('Query Results:' + queryResult.rows.length + " written to file ./outputs/runOrgStationQueryFinalSaveToFile.json");
            /*
            queryResult.rows.forEach((row) =>
            {
                console.log(row);
            });
            */
            let endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\trows:" + queryResult.rows.length + " in " + (endTime - startTime) + " ms.");
        });
    }
}
exports.CbRunQueries = CbRunQueries;
