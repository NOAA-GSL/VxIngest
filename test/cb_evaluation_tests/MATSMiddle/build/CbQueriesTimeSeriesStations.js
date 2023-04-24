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
    processStationQuery() {
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
            let stationNames = JSON.parse(fs_1.default.readFileSync("./dataFiles/station_names_all.json", 'utf-8'));
            // App.log(LogLevel.DEBUG, "station_names:\n" + JSON.stringify(stationNames, null, 2));
            let tmpl_get_N_stations_mfve_obs = fs_1.default.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_obs.sql", 'utf-8');
            // ==============================  OBS =====================================================
            let stationNames_obs = "";
            for (let i = 0; i < stationNames.length; i++) {
                if (i === 0) {
                    stationNames_obs = "obs.data." + stationNames[i];
                }
                else {
                    stationNames_obs += ",obs.data." + stationNames[i];
                }
            }
            endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tstationNames_obs:" + stationNames_obs.length + " in " + (endTime - startTime) + " ms.");
            App_1.App.log(App_1.LogLevel.DEBUG, "\tstationNames_obs:\n" + stationNames_obs);
            let tmplWithStationNames_obs = tmpl_get_N_stations_mfve_obs.replace(/{{stationNamesList}}/g, stationNames_obs);
            for (let imfve = 0; imfve < qr_fcstValidEpoch.rows.length; imfve++) {
                let sql = tmplWithStationNames_obs.replace(/{{fcstValidEpoch}}/g, qr_fcstValidEpoch.rows[imfve].fcstValidEpoch);
                // App.log(LogLevel.INFO, "sql:\n" + sql);
                const qr = yield this.bucket.scope('_default').query(sql, {
                    parameters: [],
                });
                let mfve_stations = {};
                let stationNames = Object.keys(qr.rows[0]);
                for (let i = 0; i < stationNames.length; i++) {
                    let station = qr.rows[0][stationNames[i]];
                    if (i === 0) {
                        // App.log(LogLevel.DEBUG, "station:\n" + JSON.stringify(station, null, 2));
                    }
                    mfve_stations[stationNames[i]] = station.Ceiling;
                }
                obs_mfve[qr_fcstValidEpoch.rows[imfve].fcstValidEpoch] = mfve_stations;
                if ((imfve % 100) == 0) {
                    endTime = (new Date()).valueOf();
                    App_1.App.log(App_1.LogLevel.DEBUG, "imfve:" + imfve + "/" + qr_fcstValidEpoch.rows.length + " in " + (endTime - startTime) + " ms.");
                }
            }
            endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.DEBUG, "obs_mfve:\n" + JSON.stringify(obs_mfve, null, 2) + " in " + (endTime - startTime) + " ms.");
            // ==============================  MODEL =====================================================
            let tmpl_get_N_stations_mfve_model = fs_1.default.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_model.sql", 'utf-8');
            tmpl_get_N_stations_mfve_model = tmpl_get_N_stations_mfve_obs.replace(/{{model}}/g, "HRRR_OPS");
            var stationNames_models = "";
            for (let i = 0; i < stationNames.length; i++) {
                if (i === 0) {
                    stationNames_models = "models.data." + stationNames[i];
                }
                else {
                    stationNames_models += ",models.data." + stationNames[i];
                }
            }
            endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tstationNames_models:" + stationNames_models.length + " in " + (endTime - startTime) + " ms.");
            App_1.App.log(App_1.LogLevel.DEBUG, "\tstationNames_models:\n" + stationNames_models);
            let tmplWithStationNames_models = tmpl_get_N_stations_mfve_model.replace(/{{stationNamesList}}/g, stationNames_models);
            for (let imfve = 0; imfve < qr_fcstValidEpoch.rows.length; imfve++) {
                let sql = tmplWithStationNames_models.replace(/{{fcstValidEpoch}}/g, qr_fcstValidEpoch.rows[imfve].fcstValidEpoch);
                // App.log(LogLevel.INFO, "sql:\n" + sql);
                const qr = yield this.bucket.scope('_default').query(sql, {
                    parameters: [],
                });
                let mfve_stations = {};
                let stationNames = Object.keys(qr.rows[0]);
                for (let i = 0; i < stationNames.length; i++) {
                    let station = qr.rows[0][stationNames[i]];
                    if (i === 0) {
                        App_1.App.log(App_1.LogLevel.DEBUG, "station:\n" + JSON.stringify(station, null, 2));
                    }
                    mfve_stations[stationNames[i]] = station.Ceiling;
                }
                models_mfve[qr_fcstValidEpoch.rows[imfve].fcstValidEpoch] = mfve_stations;
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
