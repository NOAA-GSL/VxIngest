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
exports.CbQueriesTimeSeriesStations = void 0;
const fs_1 = __importDefault(require("fs"));
const App_1 = require("./App");
const couchbase_1 = require("couchbase");
class CbQueriesTimeSeriesStations {
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
        this.stationNames = null;
        this.fcstValidEpoch_Array = [];
        this.fveObs = {};
        this.fveModels = {};
        this.stats = [];
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
    processStationQuery(stationsFile, model, fcstLen, threshold, writeOutput) {
        return __awaiter(this, void 0, void 0, function* () {
            App_1.App.log(App_1.LogLevel.INFO, "processStationQuery1()");
            let startTime = (new Date()).valueOf();
            let sqlstrObs = fs_1.default.readFileSync("./SQLs/get_distinct_fcstValidEpoch_obs.sql", 'utf-8');
            const qr_fcstValidEpoch = yield this.bucket.scope('_default').query(sqlstrObs, {
                parameters: [],
            });
            for (let imfve = 0; imfve < qr_fcstValidEpoch.rows.length; imfve++) {
                this.fcstValidEpoch_Array.push(qr_fcstValidEpoch.rows[imfve].fcstValidEpoch);
            }
            let endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.DEBUG, "\tfcstValidEpoch_Array:" + JSON.stringify(this.fcstValidEpoch_Array, null, 2) + " in " + (endTime - startTime) + " ms.");
            this.stationNames = JSON.parse(fs_1.default.readFileSync(stationsFile, 'utf-8'));
            // App.log(LogLevel.DEBUG, "station_names:\n" + JSON.stringify(this.stationNames, null, 2));
            yield this.createObsData();
            yield this.createModelData(model, fcstLen, threshold);
            yield this.generateStats(threshold);
            if (true === writeOutput) {
                fs_1.default.writeFileSync('./output/stats.json', JSON.stringify(this.stats, null, 2));
                fs_1.default.writeFileSync('./output/fveObs.json', JSON.stringify(this.fveObs, null, 2));
                fs_1.default.writeFileSync('./output/fveModels.json', JSON.stringify(this.fveModels, null, 2));
                console.log("Output written to files: ./outputs/stats.json, ./output/fveObs.json, ./output/fveModels.json");
            }
            endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tprocessStationQuery in " + (endTime - startTime) + " ms.");
        });
    }
    createObsData() {
        return __awaiter(this, void 0, void 0, function* () {
            let startTime = (new Date()).valueOf();
            // ==============================  OBS =====================================================
            let tmpl_get_N_stations_mfve_obs = fs_1.default.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_IN_obs.sql", 'utf-8');
            let stationNames_obs = "";
            for (let i = 0; i < this.stationNames.length; i++) {
                if (i === 0) {
                    stationNames_obs = "obs.data." + this.stationNames[i] + ".Ceiling " + this.stationNames[i];
                }
                else {
                    stationNames_obs += ",obs.data." + this.stationNames[i] + ".Ceiling " + this.stationNames[i];
                }
            }
            let endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tstationNames_obs:" + stationNames_obs.length + " in " + (endTime - startTime) + " ms.");
            // App.log(LogLevel.DEBUG, "\tstationNames_obs:\n" + stationNames_obs);
            let tmplWithStationNames_obs = tmpl_get_N_stations_mfve_obs.replace(/{{stationNamesList}}/g, stationNames_obs);
            for (let imfve = 0; imfve < this.fcstValidEpoch_Array.length; imfve = imfve + 100) {
                let fveArraySlice = this.fcstValidEpoch_Array.slice(imfve, imfve + 100);
                let sql = tmplWithStationNames_obs.replace(/{{fcstValidEpoch}}/g, JSON.stringify(fveArraySlice));
                if (imfve === 0) {
                    App_1.App.log(App_1.LogLevel.INFO, "sql:\n" + sql);
                }
                const qr = yield this.bucket.scope('_default').query(sql, {
                    parameters: [],
                });
                App_1.App.log(App_1.LogLevel.DEBUG, "qr:\n" + qr.rows.length);
                for (let jmfve = 0; jmfve < qr.rows.length; jmfve++) {
                    let fveDataSingleEpoch = qr.rows[jmfve];
                    // App.log(LogLevel.DEBUG, "mfveData:\n" + JSON.stringify(mfveData, null, 2));
                    let stationsSingleEpoch = {};
                    for (let i = 0; i < this.stationNames.length; i++) {
                        let varValStation = fveDataSingleEpoch[this.stationNames[i]];
                        if (i === 0) {
                            // App.log(LogLevel.DEBUG, "station:\n" + JSON.stringify(station, null, 2));
                        }
                        stationsSingleEpoch[this.stationNames[i]] = varValStation;
                    }
                    this.fveObs[fveDataSingleEpoch.fcstValidEpoch] = stationsSingleEpoch;
                    if (fveDataSingleEpoch.fcstValidEpoch === 1662508800) {
                        App_1.App.log(App_1.LogLevel.DEBUG, "fveDataSingleEpoch:\n" + JSON.stringify(fveDataSingleEpoch, null, 2) + "\n" +
                            JSON.stringify(this.fveObs[fveDataSingleEpoch.fcstValidEpoch]));
                        // App.log(LogLevel.DEBUG, "fveObs:\n" + JSON.stringify(this.fveObs, null, 2) );
                    }
                }
                if ((imfve % 100) == 0) {
                    endTime = (new Date()).valueOf();
                    App_1.App.log(App_1.LogLevel.DEBUG, "imfve:" + imfve + "/" + this.fcstValidEpoch_Array.length + " in " + (endTime - startTime) + " ms.");
                }
            }
            endTime = (new Date()).valueOf();
            // App.log(LogLevel.DEBUG, "obs_mfve:\n" + JSON.stringify(obs_mfve, null, 2) + " in " + (endTime - startTime) + " ms.");
        });
    }
    createModelData(model, fcstLen, threshold) {
        return __awaiter(this, void 0, void 0, function* () {
            let startTime = (new Date()).valueOf();
            let tmpl_get_N_stations_mfve_model = fs_1.default.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_IN_model.sql", 'utf-8');
            tmpl_get_N_stations_mfve_model = tmpl_get_N_stations_mfve_model.replace(/{{model}}/g, "\"" + model + "\"");
            tmpl_get_N_stations_mfve_model = tmpl_get_N_stations_mfve_model.replace(/{{fcstLen}}/g, fcstLen);
            var stationNames_models = "";
            for (let i = 0; i < this.stationNames.length; i++) {
                if (i === 0) {
                    stationNames_models = "models.data." + this.stationNames[i] + ".Ceiling " + this.stationNames[i];
                }
                else {
                    stationNames_models += ",models.data." + this.stationNames[i] + ".Ceiling " + this.stationNames[i];
                }
            }
            let endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tstationNames_models:" + stationNames_models.length + " in " + (endTime - startTime) + " ms.");
            // App.log(LogLevel.DEBUG, "\tstationNames_models:\n" + stationNames_models);
            let tmplWithStationNames_models = tmpl_get_N_stations_mfve_model.replace(/{{stationNamesList}}/g, stationNames_models);
            for (let imfve = 0; imfve < this.fcstValidEpoch_Array.length; imfve = imfve + 100) {
                let fveArraySlice = this.fcstValidEpoch_Array.slice(imfve, imfve + 100);
                let sql = tmplWithStationNames_models.replace(/{{fcstValidEpoch}}/g, JSON.stringify(fveArraySlice));
                if (imfve === 0) {
                    //App.log(LogLevel.INFO, "sql:\n" + sql);
                }
                const qr = yield this.bucket.scope('_default').query(sql, {
                    parameters: [],
                });
                for (let jmfve = 0; jmfve < qr.rows.length; jmfve++) {
                    let fveDataSingleEpoch = qr.rows[jmfve];
                    // App.log(LogLevel.DEBUG, "mfveData:\n" + JSON.stringify(mfveData, null, 2));
                    let stationsSingleEpoch = {};
                    for (let i = 0; i < this.stationNames.length; i++) {
                        let varValStation = fveDataSingleEpoch[this.stationNames[i]];
                        if (i === 0) {
                            // App.log(LogLevel.DEBUG, "station:\n" + JSON.stringify(station, null, 2));
                        }
                        stationsSingleEpoch[this.stationNames[i]] = varValStation;
                    }
                    this.fveModels[fveDataSingleEpoch.fcstValidEpoch] = stationsSingleEpoch;
                    if (fveDataSingleEpoch.fcstValidEpoch === 1662508800) {
                        // App.log(LogLevel.DEBUG, "fveDataSingleEpoch:\n" + JSON.stringify(fveDataSingleEpoch, null, 2) + "\n" +
                        //    JSON.stringify(this.fveModels[fveDataSingleEpoch.fcstValidEpoch]));
                        // App.log(LogLevel.DEBUG, "fveObs:\n" + JSON.stringify(this.fveObs, null, 2) );
                    }
                }
                if ((imfve % 100) == 0) {
                    endTime = (new Date()).valueOf();
                    App_1.App.log(App_1.LogLevel.DEBUG, "imfve:" + imfve + "/" + this.fcstValidEpoch_Array.length + " in " + (endTime - startTime) + " ms.");
                }
            }
        });
    }
    generateStats(threshold) {
        for (let imfve = 0; imfve < this.fcstValidEpoch_Array.length; imfve++) {
            let fve = this.fcstValidEpoch_Array[imfve];
            let obsSingleFve = this.fveObs[fve];
            let modelSingleFve = this.fveModels[fve];
            if (!obsSingleFve || !modelSingleFve) {
                console.log("no data for fve:" + fve + ",obsSingleFve:" + obsSingleFve + ",modelSingleFve:" + modelSingleFve);
                continue;
            }
            let stats_fve = {};
            stats_fve["avtime"] = fve;
            stats_fve["total"] = 0;
            stats_fve["hits"] = 0;
            stats_fve["misses"] = 0;
            stats_fve["fa"] = 0;
            stats_fve["cn"] = 0;
            stats_fve["N0"] = 0;
            stats_fve["N_times"] = 0;
            stats_fve["sub_data"] = [];
            for (let i = 0; i < this.stationNames.length; i++) {
                let station = this.stationNames[i];
                let varVal_o = obsSingleFve[station];
                let varVal_m = modelSingleFve[station];
                if (fve === 1662508800) {
                    // console.log("obsSingleFve:" + JSON.stringify(obsSingleFve, null, 2));
                    // console.log("modelSingleFve:" + JSON.stringify(modelSingleFve, null, 2));
                }
                // console.log("obs_mfve[mfveVal]:" + JSON.stringify(obs_mfve[mfveVal]) + ":stationNames[i]:" + stationNames[i] + ":" + obs_mfve[mfveVal][stationNames[i]]);
                if (varVal_o && varVal_m) {
                    // console.log("varVal_o:" + varVal_o + ",varVal_m:" + varVal_m);
                    stats_fve["total"] = stats_fve["total"] + 1;
                    let sub = fve + ';';
                    if (varVal_o < threshold && varVal_m < threshold) {
                        stats_fve["hits"] = stats_fve["hits"] + 1;
                        sub += "1;";
                    }
                    else {
                        sub += "0;";
                    }
                    if (fve === 1662508800) {
                        console.log("station:" + station + ",varVal_o:" + varVal_o + ",varVal_m:" + varVal_m);
                    }
                    if (varVal_o >= threshold && varVal_m < threshold) {
                        stats_fve["fa"] = stats_fve["fa"] + 1;
                        sub += "1;";
                    }
                    else {
                        sub += "0;";
                    }
                    if (varVal_o < threshold && varVal_m >= threshold) {
                        stats_fve["misses"] = stats_fve["misses"] + 1;
                        sub += "1;";
                    }
                    else {
                        sub += "0;";
                    }
                    if (varVal_o >= threshold && varVal_m >= threshold) {
                        stats_fve["cn"] = stats_fve["cn"] + 1;
                        sub += "1";
                    }
                    else {
                        sub += "0";
                    }
                    stats_fve["sub_data"].push(sub);
                }
            }
            this.stats.push(stats_fve);
        }
    }
}
exports.CbQueriesTimeSeriesStations = CbQueriesTimeSeriesStations;
