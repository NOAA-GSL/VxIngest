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


class CbRunQueries
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

    public async runQueryFile(queryFile: string, writeOutput: boolean)
    {
        App.log(LogLevel.INFO, "runQueryFile(" + queryFile + ")");

        let startTime: number = (new Date()).valueOf();

        var sqlstr = fs.readFileSync("./SQLs/" + queryFile, 'utf-8');

        const queryResult: QueryResult = await this.bucket.scope('_default').query(sqlstr, {
            parameters: [],
        });

        if (true === writeOutput)
        {
            fs.writeFileSync('./output/queryResult.json', "");
            let fd = fs.openSync('./output/queryResult.json', 'a');
            let lines = 0;
            queryResult.rows.forEach((row) =>
            {
                fs.writeSync(fd, JSON.stringify(row, null, 2));
                if ((++lines % 1000) === 0)
                {
                    console.log("\tcount:" + lines);
                }
            });
            fs.closeSync(fd);
            console.log('Query Results (Obs):' + queryResult.rows.length + " written to file ./outputs/queryResult.json");
        }
        else
        {
            console.log('Query Results (Obs):' + queryResult.rows.length);
        }

        let endTime: number = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\trows:" + queryResult.rows.length + " in " + (endTime - startTime) + " ms.");
    }

    public async runObsModelQueries(obsSqlFile: string, modelSqlFile: string, writeOutput: boolean)
    {
        App.log(LogLevel.INFO, "runObsModelQueries(" + obsSqlFile + "," + modelSqlFile + ")");

        let obs = {};
        let model = {};
        let startTime: number = (new Date()).valueOf();

        var sqlstrObs = fs.readFileSync("./SQLs/" + obsSqlFile, 'utf-8');
        const queryResultObs: QueryResult = await this.bucket.scope('_default').query(sqlstrObs, {
            parameters: [],
        });
        if (true === writeOutput)
        {
            fs.writeFileSync('./output/queryResultObs.json', "");
            let fd = fs.openSync('./output/queryResultObs.json', 'a');
            let lines = 0;
            queryResultObs.rows.forEach((row) =>
            {
                fs.writeSync(fd, JSON.stringify(row, null, 2));
                if ((++lines % 1000) === 0)
                {
                    console.log("\tcount:" + lines);
                }
            });
            fs.closeSync(fd);
            console.log('Query Results (Obs):' + queryResultObs.rows.length + " written to file ./outputs/queryResultObs.json");
        }
        else
        {
            console.log('Query Results (Obs):' + queryResultObs.rows.length);
        }
        let endTime: number = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tqueryResultObs rows:" + queryResultObs.rows.length + " in " + (endTime - startTime) + " ms.");

        var sqlstrModel = fs.readFileSync("./SQLs/" + modelSqlFile, 'utf-8');
        const queryResultModel: QueryResult = await this.bucket.scope('_default').query(sqlstrModel, {
            parameters: [],
        });
        if (true === writeOutput)
        {
            fs.writeFileSync('./output/queryResultModel.json', "");
            let fd = fs.openSync('./output/queryResultModel.json', 'a');
            let lines = 0;
            queryResultModel.rows.forEach((row) =>
            {
                fs.writeSync(fd, JSON.stringify(row, null, 2));
                if ((++lines % 1000) === 0)
                {
                    console.log("\tcount:" + lines);
                }
            });
            fs.closeSync(fd);
            console.log('Query Results (Model):' + queryResultModel.rows.length + " written to file ./outputs/queryResultModel.json");
        }
        else
        {
            console.log('Query Results (Model):' + queryResultModel.rows.length);
        }

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

        /*
        queryResult.rows.forEach((row) =>
        {
            console.log(row);
        });
        */

        let endTime: number = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\trows:" + queryResult.rows.length + " in " + (endTime - startTime) + " ms.");
    }
}

export { CbRunQueries };

