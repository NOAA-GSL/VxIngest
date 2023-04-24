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
    public settingsFile = '/Users/gopa.padmanabhan/mats-settings/configurations/dev/settings/cb-ceiling/settings.json';

    // public clusterConnStr: string = 'couchbase://localhost'
    public clusterConnStr: string = 'adb-cb1.gsd.esrl.noaa.gov';
    public bucketName: string = 'vxdata'
    public cluster: any = null;
    public bucket: any = null;

    public collection: any = null;

    public async init()
    {
        var settings = JSON.parse(fs.readFileSync(this.settingsFile, 'utf-8'));
        // App.log(LogLevel.INFO, "settings:" + JSON.stringify(settings, undefined, 2));
        App.log(LogLevel.INFO, "user:" + settings.private.databases[0].user);

        this.cluster = await connect(this.clusterConnStr, {
            username: settings.private.databases[0].user,
            password: settings.private.databases[0].password,
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

    public async processStationQuery()
    {
        App.log(LogLevel.INFO, "processStationQuery1()");

        let obs_mfve: any = {};
        let models_mfve: any = {};
        let startTime: number = (new Date()).valueOf();

        let sqlstrObs = fs.readFileSync("./SQLs/get_distinct_fcstValidEpoch_obs.sql", 'utf-8');
        const qr_fcstValidEpoch: QueryResult = await this.bucket.scope('_default').query(sqlstrObs, {
            parameters: [],
        });
        let endTime = (new Date()).valueOf();
        App.log(LogLevel.DEBUG, "\tqr_fcstValidEpoch:" + JSON.stringify(qr_fcstValidEpoch.rows.length, null, 2) + " in " + (endTime - startTime) + " ms.");

        let stationNames = JSON.parse(fs.readFileSync("./dataFiles/station_names_all.json", 'utf-8'));
        // App.log(LogLevel.DEBUG, "station_names:\n" + JSON.stringify(stationNames, null, 2));

        let tmpl_get_N_stations_mfve_obs = fs.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_obs.sql", 'utf-8');

        // ==============================  OBS =====================================================
        let stationNames_obs = "";
        for (let i = 0; i < stationNames.length; i++)
        {
            if (i === 0)
            {
                stationNames_obs = "obs.data." + stationNames[i];
            }
            else
            {
                stationNames_obs += ",obs.data." + stationNames[i];
            }
        }
        endTime = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tstationNames_obs:" + stationNames_obs.length + " in " + (endTime - startTime) + " ms.");
        App.log(LogLevel.DEBUG, "\tstationNames_obs:\n" + stationNames_obs);

        let tmplWithStationNames_obs = tmpl_get_N_stations_mfve_obs.replace(/{{stationNamesList}}/g, stationNames_obs);

        for (let imfve = 0; imfve < qr_fcstValidEpoch.rows.length; imfve++)
        {
            let sql = tmplWithStationNames_obs.replace(/{{fcstValidEpoch}}/g, qr_fcstValidEpoch.rows[imfve].fcstValidEpoch);
            // App.log(LogLevel.INFO, "sql:\n" + sql);
            const qr: QueryResult = await this.bucket.scope('_default').query(sql, {
                parameters: [],
            });
            
            let mfve_stations: any = {};
            let stationNames = Object.keys(qr.rows[0]);
            for (let i = 0; i < stationNames.length; i++)
            {
                let station = qr.rows[0][stationNames[i]];
                if (i === 0)
                {
                    // App.log(LogLevel.DEBUG, "station:\n" + JSON.stringify(station, null, 2));
                }
                mfve_stations[stationNames[i]] = station.Ceiling;
            }
            obs_mfve[qr_fcstValidEpoch.rows[imfve].fcstValidEpoch] = mfve_stations;
            if((imfve % 100) == 0)
            {
                endTime = (new Date()).valueOf();
                App.log(LogLevel.DEBUG, "imfve:" + imfve + "/" + qr_fcstValidEpoch.rows.length + " in " + (endTime - startTime) + " ms.");
            }
        }
        endTime = (new Date()).valueOf();
        App.log(LogLevel.DEBUG, "obs_mfve:\n" + JSON.stringify(obs_mfve, null, 2) + " in " + (endTime - startTime) + " ms.");

        // ==============================  MODEL =====================================================
        let tmpl_get_N_stations_mfve_model = fs.readFileSync("./sqlTemplates/tmpl_get_N_stations_mfve_model.sql", 'utf-8');
        tmpl_get_N_stations_mfve_model = tmpl_get_N_stations_mfve_obs.replace(/{{model}}/g, "HRRR_OPS");
        var stationNames_models = "";
        for (let i = 0; i < stationNames.length; i++)
        {
            if (i === 0)
            {
                stationNames_models = "models.data." + stationNames[i];
            }
            else
            {
                stationNames_models += ",models.data." + stationNames[i];
            }
        }
        endTime = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tstationNames_models:" + stationNames_models.length + " in " + (endTime - startTime) + " ms.");
        App.log(LogLevel.DEBUG, "\tstationNames_models:\n" + stationNames_models);

        let tmplWithStationNames_models = tmpl_get_N_stations_mfve_model.replace(/{{stationNamesList}}/g, stationNames_models);

        for (let imfve = 0; imfve < qr_fcstValidEpoch.rows.length; imfve++)
        {
            let sql = tmplWithStationNames_models.replace(/{{fcstValidEpoch}}/g, qr_fcstValidEpoch.rows[imfve].fcstValidEpoch);
            // App.log(LogLevel.INFO, "sql:\n" + sql);
            const qr: QueryResult = await this.bucket.scope('_default').query(sql, {
                parameters: [],
            });
            
            let mfve_stations: any = {};
            let stationNames = Object.keys(qr.rows[0]);
            for (let i = 0; i < stationNames.length; i++)
            {
                let station = qr.rows[0][stationNames[i]];
                if (i === 0)
                {
                    App.log(LogLevel.DEBUG, "station:\n" + JSON.stringify(station, null, 2));
                }
                mfve_stations[stationNames[i]] = station.Ceiling;
            }
            models_mfve[qr_fcstValidEpoch.rows[imfve].fcstValidEpoch] = mfve_stations;
            if((imfve % 100) == 0)
            {
                endTime = (new Date()).valueOf();
                App.log(LogLevel.DEBUG, "imfve:" + imfve + "/" + qr_fcstValidEpoch.rows.length + " in " + (endTime - startTime) + " ms.");
            }
        }
        App.log(LogLevel.DEBUG, "models_mfve:\n" + JSON.stringify(models_mfve, null, 2));

        endTime = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tprocessStationQuery in " + (endTime - startTime) + " ms.");
    }

}

export { CbQueriesTimeSeriesStations };

