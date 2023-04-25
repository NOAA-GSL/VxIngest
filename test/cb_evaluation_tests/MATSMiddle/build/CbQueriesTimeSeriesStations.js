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
        this.settingsFile = '/Users/gopa.padmanabhan/mats-settings/configurations/dev/settings/cb-ceiling/settings.json';
        // public clusterConnStr: string = 'couchbase://localhost'
        this.clusterConnStr = 'adb-cb1.gsd.esrl.noaa.gov';
        this.bucketName = 'vxdata';
        this.cluster = null;
        this.bucket = null;
        this.collection = null;
    }
    init() {
        return __awaiter(this, void 0, void 0, function* () {
            var settings = JSON.parse(fs_1.default.readFileSync(this.settingsFile, 'utf-8'));
            // App.log(LogLevel.INFO, "settings:" + JSON.stringify(settings, undefined, 2));
            App_1.App.log(App_1.LogLevel.INFO, "user:" + settings.private.databases[0].user);
            this.cluster = yield (0, couchbase_1.connect)(this.clusterConnStr, {
                username: settings.private.databases[0].user,
                password: settings.private.databases[0].password,
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
    processStationQuery(stationsFile, model, fcstLen, threshold) {
        return __awaiter(this, void 0, void 0, function* () {
            App_1.App.log(App_1.LogLevel.INFO, "processStationQuery1()");
            let obs_mfve = {};
            let models_mfve = {};
            let stats = [];
            let startTime = (new Date()).valueOf();
            let sqlstrObs = fs_1.default.readFileSync("./SQLs/get_distinct_fcstValidEpoch_obs.sql", 'utf-8');
            const qr_fcstValidEpoch = yield this.bucket.scope('_default').query(sqlstrObs, {
                parameters: [],
            });
            let endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.DEBUG, "\tqr_fcstValidEpoch:" + JSON.stringify(qr_fcstValidEpoch.rows.length, null, 2) + " in " + (endTime - startTime) + " ms.");
            let stationNames = JSON.parse(fs_1.default.readFileSync(stationsFile, 'utf-8'));
            // App.log(LogLevel.DEBUG, "station_names:\n" + JSON.stringify(stationNames, null, 2));
            // ==============================  OBS =====================================================
            let tmpl_get_N_stations_mfve_obs = fs_1.default.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_obs.sql", 'utf-8');
            let stationNames_obs = "";
            for (let i = 0; i < stationNames.length; i++) {
                if (i === 0) {
                    stationNames_obs = "obs.data." + stationNames[i] + ".Ceiling " + stationNames[i];
                }
                else {
                    stationNames_obs += ",obs.data." + stationNames[i] + ".Ceiling " + stationNames[i];
                }
            }
            endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tstationNames_obs:" + stationNames_obs.length + " in " + (endTime - startTime) + " ms.");
            App_1.App.log(App_1.LogLevel.DEBUG, "\tstationNames_obs:\n" + stationNames_obs);
            let tmplWithStationNames_obs = tmpl_get_N_stations_mfve_obs.replace(/{{stationNamesList}}/g, stationNames_obs);
            for (let imfve = 0; imfve < qr_fcstValidEpoch.rows.length; imfve++) {
                let mfveVal = qr_fcstValidEpoch.rows[imfve].fcstValidEpoch;
                let sql = tmplWithStationNames_obs.replace(/{{fcstValidEpoch}}/g, mfveVal);
                if (imfve === 0) {
                    // App.log(LogLevel.INFO, "sql:\n" + sql);
                }
                const qr = yield this.bucket.scope('_default').query(sql, {
                    parameters: [],
                });
                if (qr && qr.rows && qr.rows[0]) {
                    let mfve_stations = {};
                    for (let i = 0; i < stationNames.length; i++) {
                        let station = qr.rows[0][stationNames[i]];
                        if (i === 0) {
                            // App.log(LogLevel.DEBUG, "station:\n" + JSON.stringify(station, null, 2));
                        }
                        mfve_stations[stationNames[i]] = qr.rows[0][stationNames[i]];
                    }
                    obs_mfve[mfveVal] = mfve_stations;
                }
                if ((imfve % 100) == 0) {
                    endTime = (new Date()).valueOf();
                    App_1.App.log(App_1.LogLevel.DEBUG, "imfve:" + imfve + "/" + qr_fcstValidEpoch.rows.length + " in " + (endTime - startTime) + " ms.");
                }
            }
            endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.DEBUG, "obs_mfve:\n" + JSON.stringify(obs_mfve, null, 2) + " in " + (endTime - startTime) + " ms.");
            // ==============================  MODEL =====================================================
            let tmpl_get_N_stations_mfve_model = fs_1.default.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_model.sql", 'utf-8');
            tmpl_get_N_stations_mfve_model = tmpl_get_N_stations_mfve_model.replace(/{{model}}/g, "\"" + model + "\"");
            tmpl_get_N_stations_mfve_model = tmpl_get_N_stations_mfve_model.replace(/{{fcstLen}}/g, fcstLen);
            var stationNames_models = "";
            for (let i = 0; i < stationNames.length; i++) {
                if (i === 0) {
                    stationNames_models = "models.data." + stationNames[i] + ".Ceiling " + stationNames[i];
                }
                else {
                    stationNames_models += ",models.data." + stationNames[i] + ".Ceiling " + stationNames[i];
                }
            }
            endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tstationNames_models:" + stationNames_models.length + " in " + (endTime - startTime) + " ms.");
            App_1.App.log(App_1.LogLevel.DEBUG, "\tstationNames_models:\n" + stationNames_models);
            let tmplWithStationNames_models = tmpl_get_N_stations_mfve_model.replace(/{{stationNamesList}}/g, stationNames_models);
            for (let imfve = 0; imfve < qr_fcstValidEpoch.rows.length; imfve++) {
                let mfveVal = qr_fcstValidEpoch.rows[imfve].fcstValidEpoch;
                let sql = tmplWithStationNames_models.replace(/{{fcstValidEpoch}}/g, mfveVal);
                if (imfve === 0) {
                    App_1.App.log(App_1.LogLevel.INFO, "sql:\n" + sql);
                }
                const qr = yield this.bucket.scope('_default').query(sql, {
                    parameters: [],
                });
                if (imfve === 0) {
                    App_1.App.log(App_1.LogLevel.DEBUG, "qr:\n" + JSON.stringify(qr.rows[0], null, 2));
                }
                if (qr && qr.rows && qr.rows[0]) {
                    let stats_mfve = {};
                    stats_mfve["avtime"] = mfveVal;
                    stats_mfve["total"] = 0;
                    stats_mfve["hits"] = 0;
                    stats_mfve["misses"] = 0;
                    stats_mfve["fa"] = 0;
                    stats_mfve["cn"] = 0;
                    stats_mfve["N0"] = 0;
                    stats_mfve["N_times"] = 0;
                    stats_mfve["sub_data"] = [];
                    let mfve_stations = {};
                    for (let i = 0; i < stationNames.length; i++) {
                        let varVal_m = qr.rows[0][stationNames[i]];
                        let varVal_o = obs_mfve[mfveVal][stationNames[i]];
                        // console.log("obs_mfve[mfveVal]:" + JSON.stringify(obs_mfve[mfveVal]) + ":stationNames[i]:" + stationNames[i] + ":" + obs_mfve[mfveVal][stationNames[i]]);
                        if (varVal_o && varVal_m) {
                            // console.log("varVal_o:" + varVal_o + ",varVal_m:" + varVal_m);
                            mfve_stations[stationNames[i]] = varVal_m;
                            if (varVal_o < threshold && varVal_m < threshold) {
                                stats_mfve["hits"] = stats_mfve["hits"] + 1;
                            }
                            if (varVal_o >= threshold && varVal_m >= threshold) {
                                stats_mfve["cn"] = stats_mfve["cn"] + 1;
                            }
                        }
                    }
                    models_mfve[mfveVal] = mfve_stations;
                    stats.push(stats_mfve);
                }
                if ((imfve % 100) == 0) {
                    endTime = (new Date()).valueOf();
                    App_1.App.log(App_1.LogLevel.DEBUG, "imfve:" + imfve + "/" + qr_fcstValidEpoch.rows.length + " in " + (endTime - startTime) + " ms.");
                }
            }
            // App.log(LogLevel.DEBUG, "models_mfve:\n" + JSON.stringify(models_mfve, null, 2));
            App_1.App.log(App_1.LogLevel.DEBUG, "stats:\n" + JSON.stringify(stats, null, 2));
            endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tprocessStationQuery in " + (endTime - startTime) + " ms.");
        });
    }
    processStationQueryA(stationsFile, model, fcstLen, threshold) {
        return __awaiter(this, void 0, void 0, function* () {
            App_1.App.log(App_1.LogLevel.INFO, "processStationQuery1()");
            let obs_mfve = {};
            let models_mfve = {};
            let startTime = (new Date()).valueOf();
            let sqlstrObs = fs_1.default.readFileSync("./SQLs/get_distinct_fcstValidEpoch_obs.sql", 'utf-8');
            const qr_fcstValidEpoch = yield this.bucket.scope('_default').query(sqlstrObs, {
                parameters: [],
            });
            let endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.DEBUG, "\tqr_fcstValidEpoch:" + JSON.stringify(qr_fcstValidEpoch.rows.length, null, 2) + " in " + (endTime - startTime) + " ms.");
            let stationNames = JSON.parse(fs_1.default.readFileSync(stationsFile, 'utf-8'));
            // App.log(LogLevel.DEBUG, "station_names:\n" + JSON.stringify(stationNames, null, 2));
            let tmpl_get_N_stations_mfve_obs = fs_1.default.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_obs.sql", 'utf-8');
            // ==============================  OBS =====================================================
            let stationNames_obs = "";
            for (let i = 0; i < stationNames.length; i++) {
                if (i === 0) {
                    stationNames_obs = "obs.data." + stationNames[i] + ".Ceiling " + stationNames[i];
                }
                else {
                    stationNames_obs += ",obs.data." + stationNames[i] + ".Ceiling " + stationNames[i];
                }
            }
            endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tstationNames_obs:" + stationNames_obs.length + " in " + (endTime - startTime) + " ms.");
            App_1.App.log(App_1.LogLevel.DEBUG, "\tstationNames_obs:\n" + stationNames_obs);
            let tmplWithStationNames_obs = tmpl_get_N_stations_mfve_obs.replace(/{{stationNamesList}}/g, stationNames_obs);
            for (let imfve = 0; imfve < qr_fcstValidEpoch.rows.length; imfve++) {
                let sql = tmplWithStationNames_obs.replace(/{{fcstValidEpoch}}/g, qr_fcstValidEpoch.rows[imfve].fcstValidEpoch);
                if (imfve === 0) {
                    // App.log(LogLevel.INFO, "sql:\n" + sql);
                }
                const qr = yield this.bucket.scope('_default').query(sql, {
                    parameters: [],
                });
                if (qr && qr.rows && qr.rows[0]) {
                    let mfve_stations = {};
                    let stationNames = Object.keys(qr.rows[0]);
                    for (let i = 0; i < stationNames.length; i++) {
                        let station = qr.rows[0][stationNames[i]];
                        if (i === 0) {
                            // App.log(LogLevel.DEBUG, "station:\n" + JSON.stringify(station, null, 2));
                        }
                        mfve_stations[stationNames[i]] = qr.rows[0][stationNames[i]];
                    }
                    obs_mfve[qr_fcstValidEpoch.rows[imfve].fcstValidEpoch] = mfve_stations;
                }
                if ((imfve % 100) == 0) {
                    endTime = (new Date()).valueOf();
                    App_1.App.log(App_1.LogLevel.DEBUG, "imfve:" + imfve + "/" + qr_fcstValidEpoch.rows.length + " in " + (endTime - startTime) + " ms.");
                }
            }
            endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.DEBUG, "obs_mfve:\n" + JSON.stringify(obs_mfve, null, 2) + " in " + (endTime - startTime) + " ms.");
            // ==============================  MODEL =====================================================
            let tmpl_get_N_stations_mfve_model = fs_1.default.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_model.sql", 'utf-8');
            tmpl_get_N_stations_mfve_model = tmpl_get_N_stations_mfve_model.replace(/{{model}}/g, "\"" + model + "\"");
            tmpl_get_N_stations_mfve_model = tmpl_get_N_stations_mfve_model.replace(/{{fcstLen}}/g, fcstLen);
            var stationNames_models = "";
            for (let i = 0; i < stationNames.length; i++) {
                if (i === 0) {
                    stationNames_models = "models.data." + stationNames[i] + ".Ceiling " + stationNames[i];
                }
                else {
                    stationNames_models += ",models.data." + stationNames[i] + ".Ceiling " + stationNames[i];
                }
            }
            endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tstationNames_models:" + stationNames_models.length + " in " + (endTime - startTime) + " ms.");
            App_1.App.log(App_1.LogLevel.DEBUG, "\tstationNames_models:\n" + stationNames_models);
            let tmplWithStationNames_models = tmpl_get_N_stations_mfve_model.replace(/{{stationNamesList}}/g, stationNames_models);
            for (let imfve = 0; imfve < qr_fcstValidEpoch.rows.length; imfve++) {
                let sql = tmplWithStationNames_models.replace(/{{fcstValidEpoch}}/g, qr_fcstValidEpoch.rows[imfve].fcstValidEpoch);
                if (imfve === 0) {
                    // App.log(LogLevel.INFO, "sql:\n" + sql);
                }
                const qr = yield this.bucket.scope('_default').query(sql, {
                    parameters: [],
                });
                if (imfve === 0) {
                    // App.log(LogLevel.DEBUG, "qr:\n" + JSON.stringify(qr.rows[0], null, 2));
                }
                if (qr && qr.rows && qr.rows[0]) {
                    let mfve_stations = {};
                    let stationNames = Object.keys(qr.rows[0]);
                    for (let i = 0; i < stationNames.length; i++) {
                        let station = qr.rows[0][stationNames[i]];
                        if (i === 0) {
                            // App.log(LogLevel.DEBUG, "station:\n" + JSON.stringify(station, null, 2));
                        }
                        mfve_stations[stationNames[i]] = qr.rows[0][stationNames[i]];
                    }
                    models_mfve[qr_fcstValidEpoch.rows[imfve].fcstValidEpoch] = mfve_stations;
                }
                if ((imfve % 100) == 0) {
                    endTime = (new Date()).valueOf();
                    App_1.App.log(App_1.LogLevel.DEBUG, "imfve:" + imfve + "/" + qr_fcstValidEpoch.rows.length + " in " + (endTime - startTime) + " ms.");
                }
            }
            App_1.App.log(App_1.LogLevel.DEBUG, "models_mfve:\n" + JSON.stringify(models_mfve, null, 2));
            endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tprocessStationQuery in " + (endTime - startTime) + " ms.");
        });
    }
}
exports.CbQueriesTimeSeriesStations = CbQueriesTimeSeriesStations;
