'use strict'

import fs from 'fs';
import { escape } from 'querystring';
import readline = require('readline');

import { LogLevel, App } from "./App";

import
{
    Bucket,
    Cluster,
    Collection,
    connect,
    GetResult,
    QueryResult,
} from 'couchbase'
import { getEventListeners } from 'stream';


class CbQueriesTimeSeriesStations
{
    public configFile = "./config/config.json";
    public config = JSON.parse(fs.readFileSync(this.configFile, 'utf-8'));
    public settings = JSON.parse(fs.readFileSync(this.config.settingsFile, 'utf-8'));

    // public clusterConnStr: string = 'couchbase://localhost'
    public clusterConnStr: string = 'adb-cb1.gsd.esrl.noaa.gov';
    public bucketName: string = 'vxdata'
    public cluster: any = null;
    public bucket: any = null;

    public collection: any = null;

    public stationNames: any = null;
    public fcstValidEpoch_Array: any = [];
    public fveObs: any = {};
    public fveModels: any = {};
    public stats: any = [];

    public async init()
    {
        // App.log(LogLevel.INFO, "settings:" + JSON.stringify(settings, undefined, 2));
        App.log(LogLevel.INFO, "user:" + this.settings.private.databases[0].user);

        this.cluster = await connect(this.clusterConnStr, {
            username: this.settings.private.databases[0].user,
            password: this.settings.private.databases[0].password,
            timeouts: {
                kvTimeout: 3600000, // this will kill queries after an hour
                queryTimeout: 3600000
            }
        })

        this.bucket = this.cluster.bucket(this.bucketName)

        // Get a reference to the default collection, required only for older Couchbase server versions
        this.collection = this.bucket.scope('_default').collection('METAR')

        App.log(LogLevel.INFO, "Connected!");
    }

    public async shutdown()
    {
    }

    public async processStationQuery(stationsFile: string, model: string, fcstLen: any, threshold: number, writeOutput: boolean)
    {
        App.log(LogLevel.INFO, "processStationQuery()");

        let startTime: number = (new Date()).valueOf();

        let sqlstrObs = fs.readFileSync("./SQLs/get_distinct_fcstValidEpoch_obs.sql", 'utf-8');
        const qr_fcstValidEpoch: QueryResult = await this.bucket.scope('_default').query(sqlstrObs, {
            parameters: [],
        });

        for (let imfve = 0; imfve < qr_fcstValidEpoch.rows.length; imfve++)
        {
            this.fcstValidEpoch_Array.push(qr_fcstValidEpoch.rows[imfve].fcstValidEpoch);
        }
        let endTime = (new Date()).valueOf();
        App.log(LogLevel.DEBUG, "\tfcstValidEpoch_Array:" + this.fcstValidEpoch_Array.length + " in " + (endTime - startTime) + " ms.");
        // App.log(LogLevel.DEBUG, "\tfcstValidEpoch_Array:" + JSON.stringify(this.fcstValidEpoch_Array, null, 2) + " in " + (endTime - startTime) + " ms.");

        this.stationNames = JSON.parse(fs.readFileSync(stationsFile, 'utf-8'));
        // App.log(LogLevel.DEBUG, "station_names:\n" + JSON.stringify(this.stationNames, null, 2));

        let prObs = this.createObsData();
        let prModel = this.createModelData(model, fcstLen, threshold);
        await Promise.all([prObs, prModel]);
        this.generateStats(threshold);

        if (true === writeOutput)
        {
            fs.writeFileSync('./output/stats.json', JSON.stringify(this.stats, null, 2));
            fs.writeFileSync('./output/fveObs.json', JSON.stringify(this.fveObs, null, 2));
            fs.writeFileSync('./output/fveModels.json', JSON.stringify(this.fveModels, null, 2));
            console.log("Output written to files: ./outputs/stats.json, ./output/fveObs.json, ./output/fveModels.json");
        }

        endTime = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tprocessStationQuery in " + (endTime - startTime) + " ms.");
    }

    public async createObsData()
    {
        App.log(LogLevel.INFO, "createObsData()");

        let startTime: number = (new Date()).valueOf();

        // ==============================  OBS =====================================================
        let tmpl_get_N_stations_mfve_obs = fs.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_IN_obs.sql", 'utf-8');
        let stationNames_obs = "";
        for (let i = 0; i < this.stationNames.length; i++)
        {
            if (i === 0)
            {
                stationNames_obs = "obs.data." + this.stationNames[i] + ".Ceiling " + this.stationNames[i];
            }
            else
            {
                stationNames_obs += ",obs.data." + this.stationNames[i] + ".Ceiling " + this.stationNames[i];
            }
        }
        let endTime = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tstationNames_obs:" + stationNames_obs.length + " in " + (endTime - startTime) + " ms.");
        // App.log(LogLevel.DEBUG, "\tstationNames_obs:\n" + stationNames_obs);

        let tmplWithStationNames_obs = tmpl_get_N_stations_mfve_obs.replace(/{{stationNamesList}}/g, stationNames_obs);

        const promises = [];
        for (let iofve = 0; iofve < this.fcstValidEpoch_Array.length; iofve = iofve + 100)
        {
            let fveArraySlice = this.fcstValidEpoch_Array.slice(iofve, iofve + 100);
            let sql = tmplWithStationNames_obs.replace(/{{fcstValidEpoch}}/g, JSON.stringify(fveArraySlice));
            if (iofve === 0)
            {
                // App.log(LogLevel.INFO, "sql:\n" + sql);
            }
            let prSlice = this.bucket.scope('_default').query(sql, {
                parameters: []
            });
            promises.push(prSlice);
            prSlice.then((qr: QueryResult) =>
            {
                App.log(LogLevel.DEBUG, "qr:\n" + qr.rows.length);
                for (let jmfve = 0; jmfve < qr.rows.length; jmfve++)
                {
                    let fveDataSingleEpoch = qr.rows[jmfve];
                    // App.log(LogLevel.DEBUG, "mfveData:\n" + JSON.stringify(mfveData, null, 2));
                    let stationsSingleEpoch: any = {};
                    for (let i = 0; i < this.stationNames.length; i++)
                    {
                        let varValStation = fveDataSingleEpoch[this.stationNames[i]];
                        if (i === 0)
                        {
                            // App.log(LogLevel.DEBUG, "station:\n" + JSON.stringify(station, null, 2));
                        }
                        stationsSingleEpoch[this.stationNames[i]] = varValStation;
                    }
                    this.fveObs[fveDataSingleEpoch.fcstValidEpoch] = stationsSingleEpoch;
                    if (fveDataSingleEpoch.fcstValidEpoch === 1662508800)
                    {
                        // App.log(LogLevel.DEBUG, "fveDataSingleEpoch:\n" + JSON.stringify(fveDataSingleEpoch, null, 2) + "\n" +
                        //    JSON.stringify(this.fveObs[fveDataSingleEpoch.fcstValidEpoch]));
                        // App.log(LogLevel.DEBUG, "fveObs:\n" + JSON.stringify(this.fveObs, null, 2) );
                    }
                }
                if ((iofve % 100) == 0)
                {
                    endTime = (new Date()).valueOf();
                    App.log(LogLevel.DEBUG, "iofve:" + iofve + "/" + this.fcstValidEpoch_Array.length + " in " + (endTime - startTime) + " ms.");
                }
            });
        }

        await Promise.all(promises);
        endTime = (new Date()).valueOf();
        App.log(LogLevel.DEBUG, "fveObs:" + " in " + (endTime - startTime) + " ms.");
    }

    public async createModelData(model: string, fcstLen: any, threshold: number)
    {
        App.log(LogLevel.INFO, "createModelData()");

        let startTime: number = (new Date()).valueOf();

        let tmpl_get_N_stations_mfve_model = fs.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_IN_model.sql", 'utf-8');
        tmpl_get_N_stations_mfve_model = tmpl_get_N_stations_mfve_model.replace(/{{model}}/g, "\"" + model + "\"");
        tmpl_get_N_stations_mfve_model = tmpl_get_N_stations_mfve_model.replace(/{{fcstLen}}/g, fcstLen);
        var stationNames_models = "";
        for (let i = 0; i < this.stationNames.length; i++)
        {
            if (i === 0)
            {
                stationNames_models = "models.data." + this.stationNames[i] + ".Ceiling " + this.stationNames[i];
            }
            else
            {
                stationNames_models += ",models.data." + this.stationNames[i] + ".Ceiling " + this.stationNames[i];
            }
        }

        let endTime = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tstationNames_models:" + stationNames_models.length + " in " + (endTime - startTime) + " ms.");
        // App.log(LogLevel.DEBUG, "\tstationNames_models:\n" + stationNames_models);

        let tmplWithStationNames_models = tmpl_get_N_stations_mfve_model.replace(/{{stationNamesList}}/g, stationNames_models);

        const promises = [];
        for (let imfve = 0; imfve < this.fcstValidEpoch_Array.length; imfve = imfve + 100)
        {
            let fveArraySlice = this.fcstValidEpoch_Array.slice(imfve, imfve + 100);
            let sql = tmplWithStationNames_models.replace(/{{fcstValidEpoch}}/g, JSON.stringify(fveArraySlice));
            if (imfve === 0)
            {
                //App.log(LogLevel.INFO, "sql:\n" + sql);
            }
            let prSlice = this.bucket.scope('_default').query(sql, {
                parameters: [],
            });

            promises.push(prSlice);
            prSlice.then((qr: QueryResult) =>
            {
                for (let jmfve = 0; jmfve < qr.rows.length; jmfve++)
                {
                    let fveDataSingleEpoch = qr.rows[jmfve];
                    // App.log(LogLevel.DEBUG, "mfveData:\n" + JSON.stringify(mfveData, null, 2));
                    let stationsSingleEpoch: any = {};
                    for (let i = 0; i < this.stationNames.length; i++)
                    {
                        let varValStation = fveDataSingleEpoch[this.stationNames[i]];
                        if (i === 0)
                        {
                            // App.log(LogLevel.DEBUG, "station:\n" + JSON.stringify(station, null, 2));
                        }
                        stationsSingleEpoch[this.stationNames[i]] = varValStation;
                    }
                    this.fveModels[fveDataSingleEpoch.fcstValidEpoch] = stationsSingleEpoch;
                    if (fveDataSingleEpoch.fcstValidEpoch === 1662508800)
                    {
                        // App.log(LogLevel.DEBUG, "fveDataSingleEpoch:\n" + JSON.stringify(fveDataSingleEpoch, null, 2) + "\n" +
                        //    JSON.stringify(this.fveModels[fveDataSingleEpoch.fcstValidEpoch]));
                        // App.log(LogLevel.DEBUG, "fveObs:\n" + JSON.stringify(this.fveObs, null, 2) );
                    }
                }
                if ((imfve % 100) == 0)
                {
                    endTime = (new Date()).valueOf();
                    App.log(LogLevel.DEBUG, "imfve:" + imfve + "/" + this.fcstValidEpoch_Array.length + " in " + (endTime - startTime) + " ms.");
                }
            });
        }
        await Promise.all(promises);
        endTime = (new Date()).valueOf();
        App.log(LogLevel.DEBUG, "fveModel:" + " in " + (endTime - startTime) + " ms.");
    }

    public generateStats(threshold: number)
    {
        App.log(LogLevel.INFO, "generateStats(" + threshold + ")");

        let startTime: number = (new Date()).valueOf();

        for (let imfve = 0; imfve < this.fcstValidEpoch_Array.length; imfve++)
        {
            let fve = this.fcstValidEpoch_Array[imfve];
            let obsSingleFve = this.fveObs[fve];
            let modelSingleFve = this.fveModels[fve];

            if (!obsSingleFve || !modelSingleFve)
            {
                // console.log("no data for fve:" + fve + ",obsSingleFve:"+ obsSingleFve + ",modelSingleFve:" + modelSingleFve);
                continue;
            }

            let stats_fve: any = {};
            stats_fve["avtime"] = fve;
            stats_fve["total"] = 0;
            stats_fve["hits"] = 0;
            stats_fve["misses"] = 0;
            stats_fve["fa"] = 0;
            stats_fve["cn"] = 0;
            stats_fve["N0"] = 0;
            stats_fve["N_times"] = 0;
            stats_fve["sub_data"] = [];

            for (let i = 0; i < this.stationNames.length; i++)
            {
                let station = this.stationNames[i];
                let varVal_o = obsSingleFve[station];
                let varVal_m = modelSingleFve[station];

                if (fve === 1662508800)
                {
                    // console.log("obsSingleFve:" + JSON.stringify(obsSingleFve, null, 2));
                    // console.log("modelSingleFve:" + JSON.stringify(modelSingleFve, null, 2));
                }
                // console.log("obs_mfve[mfveVal]:" + JSON.stringify(obs_mfve[mfveVal]) + ":stationNames[i]:" + stationNames[i] + ":" + obs_mfve[mfveVal][stationNames[i]]);

                if (varVal_o && varVal_m)
                {
                    // console.log("varVal_o:" + varVal_o + ",varVal_m:" + varVal_m);

                    stats_fve["total"] = stats_fve["total"] + 1;
                    let sub = fve + ';';
                    if (varVal_o < threshold && varVal_m < threshold)
                    {
                        stats_fve["hits"] = stats_fve["hits"] + 1;
                        sub += "1;";
                    }
                    else
                    {
                        sub += "0;";
                    }

                    if (fve === 1662508800)
                    {
                        // console.log("station:" + station + ",varVal_o:" + varVal_o + ",varVal_m:" + varVal_m);
                    }
                    if (varVal_o >= threshold && varVal_m < threshold)
                    {
                        stats_fve["fa"] = stats_fve["fa"] + 1;
                        sub += "1;";
                    }
                    else
                    {
                        sub += "0;";
                    }

                    if (varVal_o < threshold && varVal_m >= threshold)
                    {
                        stats_fve["misses"] = stats_fve["misses"] + 1;
                        sub += "1;";
                    }
                    else
                    {
                        sub += "0;";
                    }

                    if (varVal_o >= threshold && varVal_m >= threshold)
                    {
                        stats_fve["cn"] = stats_fve["cn"] + 1;
                        sub += "1";
                    }
                    else
                    {
                        sub += "0";
                    }
                    stats_fve["sub_data"].push(sub);
                }
            }
            this.stats.push(stats_fve);
        }

        let endTime = (new Date()).valueOf();
        App.log(LogLevel.DEBUG, "generateStats:" + " in " + (endTime - startTime) + " ms.");
    }
}

export { CbQueriesTimeSeriesStations };

