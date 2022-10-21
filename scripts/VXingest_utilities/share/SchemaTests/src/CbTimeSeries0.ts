'use strict'

import fs from 'fs';
import { escape } from 'querystring';
import readline = require('readline');

import { App } from "./App";

import {
Bucket,
Cluster,
Collection,
connect,
GetResult,
QueryResult,
Scope,
} from 'couchbase'
import { getEventListeners } from 'stream';


class CbTimeSeries0 {
    public async doTimeSeriesQuery0(bucket: any, query_file: string) {
        
        App.elapsed_time("CbTimeSeries0 start.");
        
        const data = fs.readFileSync(query_file, 'utf8');
        console.log(data);
        const scope: Scope = await bucket.scope('_default');
        const queryResult: QueryResult = await scope.query(data, {
                parameters: ['United States'],
            })
        console.log('Query Results:')
        queryResult.rows.forEach((row) => {
            console.log(row)
        })

        App.elapsed_time("CbTimeSeries0 end.");
    }

    public async doTimeSeriesQueryA(bucket: any, query_file_model: string, query_file_obs: string) {
        
        App.elapsed_time("CbTimeSeries0 start.");
        
        const sql_model = fs.readFileSync(query_file_model, 'utf8');
        console.log(sql_model);
        const sql_obs = fs.readFileSync(query_file_obs, 'utf8');
        console.log(sql_obs);
        const scope: Scope = await bucket.scope('_default');
        const result_model: QueryResult = await scope.query(sql_model, {
                parameters: ['United States'],
            });
        const result_obs: QueryResult = await scope.query(sql_obs, {
                parameters: ['United States'],
            })
        console.log('Query Results:')
        result_obs.rows.forEach((row) => {
            if(row.stations && Object.keys(row.stations).length > 0)
            {
                console.log(row);
            }
        })

        App.elapsed_time("CbTimeSeries0 end.");
    }
}

export { CbTimeSeries0 };

