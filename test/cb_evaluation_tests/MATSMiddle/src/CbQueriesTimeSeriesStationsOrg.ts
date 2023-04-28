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


class CbQueriesTimeSeriesStationsOrg
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
        App.log(LogLevel.INFO, "processStationQuery1()");

        let obs_mfve: any = {};
        let models_mfve: any = {};
        let stats: any = [];
        let startTime: number = (new Date()).valueOf();

        let sqlstrObs = fs.readFileSync("./SQLs/get_distinct_fcstValidEpoch_obs.sql", 'utf-8');
        const qr_fcstValidEpoch: QueryResult = await this.bucket.scope('_default').query(sqlstrObs, {
            parameters: [],
        });

        let fcstValidEpoch_Array = [];
        for (let imfve = 0; imfve < qr_fcstValidEpoch.rows.length; imfve++)
        {
            fcstValidEpoch_Array.push(qr_fcstValidEpoch.rows[imfve].fcstValidEpoch);
        }
        let endTime = (new Date()).valueOf();
        App.log(LogLevel.DEBUG, "\tfcstValidEpoch_Array:" + JSON.stringify(fcstValidEpoch_Array, null, 2) + " in " + (endTime - startTime) + " ms.");

        let stationNames = JSON.parse(fs.readFileSync(stationsFile, 'utf-8'));
        // App.log(LogLevel.DEBUG, "station_names:\n" + JSON.stringify(stationNames, null, 2));

        // ==============================  OBS =====================================================
        let tmpl_get_N_stations_mfve_obs = fs.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_IN_obs.sql", 'utf-8');
        let stationNames_obs = "";
        for (let i = 0; i < stationNames.length; i++)
        {
            if (i === 0)
            {
                stationNames_obs = "obs.data." + stationNames[i] + ".Ceiling " + stationNames[i];
            }
            else
            {
                stationNames_obs += ",obs.data." + stationNames[i] + ".Ceiling " + stationNames[i];
            }
        }
        endTime = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tstationNames_obs:" + stationNames_obs.length + " in " + (endTime - startTime) + " ms.");
        // App.log(LogLevel.DEBUG, "\tstationNames_obs:\n" + stationNames_obs);

        let tmplWithStationNames_obs = tmpl_get_N_stations_mfve_obs.replace(/{{stationNamesList}}/g, stationNames_obs);

        for (let imfve = 0; imfve < fcstValidEpoch_Array.length; imfve = imfve + 100)
        {

            let mfveArray = fcstValidEpoch_Array.slice(imfve, imfve + 100);
            let sql = tmplWithStationNames_obs.replace(/{{fcstValidEpoch}}/g, JSON.stringify(mfveArray));
            if (imfve === 0)
            {
                App.log(LogLevel.INFO, "sql:\n" + sql);
            }
            const qr: QueryResult = await this.bucket.scope('_default').query(sql, {
                parameters: [],
            });
            App.log(LogLevel.DEBUG, "qr:\n" + qr.rows.length);

            for (let jmfve = 0; jmfve < qr.rows.length; jmfve++)
            {
                let mfveData = qr.rows[jmfve];
                // App.log(LogLevel.DEBUG, "mfveData:\n" + JSON.stringify(mfveData, null, 2));
                let mfve_stations: any = {};
                for (let i = 0; i < stationNames.length; i++)
                {
                    let station = mfveData[stationNames[i]];
                    if (i === 0)
                    {
                        // App.log(LogLevel.DEBUG, "station:\n" + JSON.stringify(station, null, 2));
                    }
                    mfve_stations[stationNames[i]] = mfveData[stationNames[i]];
                }
                obs_mfve[mfveData.fcstValidEpoch] = mfve_stations;

            }
            if ((imfve % 100) == 0)
            {
                endTime = (new Date()).valueOf();
                App.log(LogLevel.DEBUG, "imfve:" + imfve + "/" + fcstValidEpoch_Array.length + " in " + (endTime - startTime) + " ms.");
            }
        }
        endTime = (new Date()).valueOf();
        // App.log(LogLevel.DEBUG, "obs_mfve:\n" + JSON.stringify(obs_mfve, null, 2) + " in " + (endTime - startTime) + " ms.");

        // ==============================  MODEL =====================================================
        let tmpl_get_N_stations_mfve_model = fs.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_IN_model.sql", 'utf-8');
        tmpl_get_N_stations_mfve_model = tmpl_get_N_stations_mfve_model.replace(/{{model}}/g, "\"" + model + "\"");
        tmpl_get_N_stations_mfve_model = tmpl_get_N_stations_mfve_model.replace(/{{fcstLen}}/g, fcstLen);
        var stationNames_models = "";
        for (let i = 0; i < stationNames.length; i++)
        {
            if (i === 0)
            {
                stationNames_models = "models.data." + stationNames[i] + ".Ceiling " + stationNames[i];
            }
            else
            {
                stationNames_models += ",models.data." + stationNames[i] + ".Ceiling " + stationNames[i];
            }
        }
        endTime = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tstationNames_models:" + stationNames_models.length + " in " + (endTime - startTime) + " ms.");
        // App.log(LogLevel.DEBUG, "\tstationNames_models:\n" + stationNames_models);

        let tmplWithStationNames_models = tmpl_get_N_stations_mfve_model.replace(/{{stationNamesList}}/g, stationNames_models);

        for (let imfve = 0; imfve < fcstValidEpoch_Array.length; imfve = imfve + 100)
        {
            let mfveArray = fcstValidEpoch_Array.slice(imfve, imfve + 100);
            let sql = tmplWithStationNames_models.replace(/{{fcstValidEpoch}}/g, JSON.stringify(mfveArray));
            if (imfve === 0)
            {
                //App.log(LogLevel.INFO, "sql:\n" + sql);
            }
            const qr: QueryResult = await this.bucket.scope('_default').query(sql, {
                parameters: [],
            });

            for (let jmfve = 0; jmfve < qr.rows.length; jmfve++)
            {
                let mfveData = qr.rows[jmfve];
                //App.log(LogLevel.DEBUG, "mfveData:\n" + JSON.stringify(mfveData, null, 2));

                if (imfve === 0)
                {
                    // App.log(LogLevel.DEBUG, "qr:\n" + JSON.stringify(qr.rows[0], null, 2));
                }

                let stats_mfve: any = {};
                stats_mfve["avtime"] = mfveData.fcstValidEpoch;
                stats_mfve["total"] = 0;
                stats_mfve["hits"] = 0;
                stats_mfve["misses"] = 0;
                stats_mfve["fa"] = 0;
                stats_mfve["cn"] = 0;
                stats_mfve["N0"] = 0;
                stats_mfve["N_times"] = 0;
                stats_mfve["sub_data"] = [];

                let mfve_stations: any = {};
                for (let i = 0; i < stationNames.length; i++)
                {
                    let varVal_o = obs_mfve[mfveData.fcstValidEpoch][stationNames[i]];
                    let varVal_m = qr.rows[jmfve][stationNames[i]];
                    if (fcstValidEpoch_Array[imfve] === 1662508800)
                    {
                        console.log(JSON.stringify(qr.rows[0], null, 2));
                        console.log("stationNames[i]:" + stationNames[i] + ",varVal_o:" + varVal_o + ",varVal_m:" + varVal_m);
                    }
                    // console.log("obs_mfve[mfveVal]:" + JSON.stringify(obs_mfve[mfveVal]) + ":stationNames[i]:" + stationNames[i] + ":" + obs_mfve[mfveVal][stationNames[i]]);

                    if (obs_mfve[mfveData.fcstValidEpoch] && obs_mfve[mfveData.fcstValidEpoch][stationNames[i]] && varVal_m)
                    {
                        // console.log("varVal_o:" + varVal_o + ",varVal_m:" + varVal_m);

                        mfve_stations[stationNames[i]] = varVal_m;

                        stats_mfve["total"] = stats_mfve["total"] + 1;
                        let sub = mfveData.fcstValidEpoch + ';';
                        if (varVal_o < threshold && varVal_m < threshold)
                        {
                            stats_mfve["hits"] = stats_mfve["hits"] + 1;
                            sub += "1;";
                        }
                        else
                        {
                            sub += "0;";
                        }

                        if (mfveData.fcstValidEpoch === 1662508800)
                        {
                            console.log("stationNames[i]:" + stationNames[i] + ",varVal_o:" + varVal_o + ",varVal_m:" + varVal_m);
                        }
                        if (varVal_o >= threshold && varVal_m < threshold)
                        {
                            stats_mfve["fa"] = stats_mfve["fa"] + 1;
                            sub += "1;";
                        }
                        else
                        {
                            sub += "0;";
                        }

                        if (varVal_o < threshold && varVal_m >= threshold)
                        {
                            stats_mfve["misses"] = stats_mfve["misses"] + 1;
                            sub += "1;";
                        }
                        else
                        {
                            sub += "0;";
                        }

                        if (varVal_o >= threshold && varVal_m >= threshold)
                        {
                            stats_mfve["cn"] = stats_mfve["cn"] + 1;
                            sub += "1";
                        }
                        else
                        {
                            sub += "0";
                        }
                        stats_mfve["sub_data"].push(sub);
                    }
                }
                models_mfve[mfveData.fcstValidEpoch] = mfve_stations;

                stats.push(stats_mfve);
            }
            if ((imfve % 100) == 0)
            {
                endTime = (new Date()).valueOf();
                App.log(LogLevel.DEBUG, "imfve:" + imfve + "/" + qr_fcstValidEpoch.rows.length + " in " + (endTime - startTime) + " ms.");
            }
        }

        if (true === writeOutput)
        {
            fs.writeFileSync('./output/stats.json', JSON.stringify(stats, null, 2));
            fs.writeFileSync('./output/obs_mfve.json', JSON.stringify(obs_mfve, null, 2));
            fs.writeFileSync('./output/models_mfve.json', JSON.stringify(models_mfve, null, 2));
            console.log("Output written to files: ./outputs/stats.json, ./output/obs_mfve.json, ./output/models_mfve.json");
        }
        else
        {
            // App.log(LogLevel.DEBUG, "models_mfve:\n" + JSON.stringify(models_mfve, null, 2));
            App.log(LogLevel.DEBUG, "stats:\n" + JSON.stringify(stats, null, 2));
        }

        endTime = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tprocessStationQuery in " + (endTime - startTime) + " ms.");
    }

}

export { CbQueriesTimeSeriesStationsOrg };

