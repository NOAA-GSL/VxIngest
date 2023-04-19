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
                kvTimeout: 10000, // milliseconds
            },
        })

        this.bucket = this.cluster.bucket(this.bucketName)

        // Get a reference to the default collection, required only for older Couchbase server versions
        this.collection = this.bucket.scope('_default').collection('METAR')

        App.log(LogLevel.INFO, "Connected!");

        
    }

    public async shutdown()
    {
    }

    public async processStationQuery_N_Stations()
    {
        App.log(LogLevel.INFO, "processStationQuery()");

        let startTime: number = (new Date()).valueOf();

        var sqlstrObs = fs.readFileSync("./SQLs/get_N_stations_obs.sql", 'utf-8');

        const queryResultObs: QueryResult = await this.bucket.scope('_default').query(sqlstrObs, {
            parameters: [],
        });

        /*
        console.log('Query Results:');
        queryResult.rows.forEach((row) =>
        {
            console.log(row);
        });
        */

        let endTime: number = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tqueryResultObs rows:" + queryResultObs.rows.length + " in " + (endTime - startTime) + " ms.");

        var sqlstrModel = fs.readFileSync("./SQLs/get_N_stations_model.sql", 'utf-8');

        const queryResultModel: QueryResult = await this.bucket.scope('_default').query(sqlstrModel, {
            parameters: [],
        });

        /*
        console.log('Query Results:');
        queryResult.rows.forEach((row) =>
        {
            console.log(row);
        });
        */

        endTime = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tqueryResultModel rows:" + queryResultModel.rows.length + " in " + (endTime - startTime) + " ms.");
    }

    public async runOrgStationQueryFinalSaveToFile()
    {
        App.log(LogLevel.INFO, "runOrgStationQueryFinalSaveToFile()");

        let startTime: number = (new Date()).valueOf();

        var sqlstr = fs.readFileSync("./SQLs/final_TimeSeries.sql", 'utf-8');

        const queryResult: QueryResult = await this.bucket.scope('_default').query(sqlstr, {
            parameters: [],
        });

        fs.writeFileSync('./output/runOrgStationQueryFinalSaveToFile.json', JSON.stringify(queryResult.rows, null, 2));
        console.log('Query Results:' + queryResult.rows.length + " written to file ./outputs/runOrgStationQueryFinalSaveToFile.json");

        /*/
        queryResult.rows.forEach((row) =>
        {
            console.log(row);
        });
        */

        let endTime: number = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\trows:" + queryResult.rows.length + " in " + (endTime - startTime) + " ms.");
    }
}

export { CbQueriesTimeSeriesStations };

