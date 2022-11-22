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


class CbUpload
{
    // public jsonFile = '/scratch/mdatatest/xaa';
    public jsonFile = '/Users/gopa.padmanabhan/scratch/mdatatest/xaa';

    // public clusterConnStr: string = 'couchbase://localhost'
    public clusterConnStr: string = 'adb-cb1.gsd.esrl.noaa.gov';
    public bucketName: string = 'vxdata'
    public cluster: any = null;
    public bucket: any = null;

    public collection_default: any = null;
    public collection_obs: any = null;
    public collection_model: any = null;
    public collection_METAR: any = null;

    public async init(confFile)
    {
        var settings = JSON.parse(fs.readFileSync(confFile, 'utf-8'));
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
        this.collection_default = this.bucket.defaultCollection()
        this.collection_obs = this.bucket.scope('_default').collection('obs')
        this.collection_model = this.bucket.scope('_default').collection('model')
        this.collection_METAR = this.bucket.scope('_default').collection('METAR')

        App.log(LogLevel.INFO, "Connected!");

        /*
        interface User
        {
            type: string
            name: string
            email: string
            interests: string[]
        }
    
        const user: User = {
            type: 'user',
            name: 'Michael',
            email: 'michael123@test.com',
            interests: ['Swimming', 'Rowing'],
        }
    
        await collection.upsert('michael123', user)
    
        // Load the Document and print it
        // Prints Content and Metadata of the stored document
        const getResult: GetResult = await collection.get('michael123')
        console.log('Get Result:', getResult)
    
        // Perform a N1QL Query
        const queryResult: QueryResult = await bucket
            .scope('inventory')
            .query('SELECT name FROM `airline` WHERE country=$1 LIMIT 10', {
                parameters: ['United States'],
            })
        console.log('Query Results:')
        queryResult.rows.forEach((row) =>
        {
            console.log(row)
        })
    
        */
        // await readJsonLines();
        // await this.uploadJsonLines(collection_default);
        // await uploadJsonLines(collection);
    }

    public async shutdown()
    {
    }

    public async readJsonAll()
    {
        var settings = JSON.parse(fs.readFileSync(this.jsonFile, 'utf-8'));
        console.log(settings);
    }

    public async readJsonLines()
    {
        console.log("readJsonLines()");

        let rs = fs.createReadStream(this.jsonFile);

        const rl = readline.createInterface({
            input: rs,
            crlfDelay: Infinity
        });
        // Note: we use the crlfDelay option to recognize all instances of CR LF
        // ('\r\n') in input.txt as a single line break.

        for await (const line of rl)
        {
            let lineObj = JSON.parse(line);
            let objStr = JSON.stringify(lineObj, undefined, 2);
            console.log(objStr);
        }
    }

    public async uploadJsonLinesDefault()
    {
        App.log(LogLevel.INFO, "uploadJsonLinesDefault()");

        let startTime: number = (new Date()).valueOf();

        let rs = fs.createReadStream(this.jsonFile);

        const rl = readline.createInterface({
            input: rs,
            crlfDelay: Infinity
        });
        // Note: we use the crlfDelay option to recognize all instances of CR LF
        // ('\r\n') in input.txt as a single line break.

        let count: number = 0;
        for await (const line of rl)
        {
            let lineObj = JSON.parse(line);

            lineObj.stations = {};

            if (lineObj.data)
            {
                for (let i = 0; i < lineObj.data.length; i++)
                {
                    lineObj.stations[lineObj.data[i].name] = lineObj.data[i];
                }
            }
            lineObj["idx0"] = lineObj.type + ":" + lineObj.subset + ":" + lineObj.version + ":" + lineObj.model;
            lineObj.data = undefined;
            let objStr = JSON.stringify(lineObj, undefined, 2);
            // App.log(LogLevel.INFO, objStr);
            // await this.collection_default.upsert(lineObj.id, lineObj)
            if (((++count) % 100) == 0)
            {
                console.log(count);
            }
        }
        let endTime: number = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tin " + (endTime - startTime) + " ms.");
    }

    /*
        set maxCount = 0, to upload all
    */
    public async uploadJsonLines(jsonFile, maxCount)
    {
        App.log(LogLevel.INFO, "uploadJsonLines()");

        let startTime: number = (new Date()).valueOf();

        let rs = fs.createReadStream(jsonFile);

        const rl = readline.createInterface({
            input: rs,
            crlfDelay: Infinity
        });
        // Note: we use the crlfDelay option to recognize all instances of CR LF
        // ('\r\n') in input.txt as a single line break.

        let count_model: number = 0;
        let count_obs: number = 0;
        let count_METAR: number = 0;
        let count_metar: number = 0;

        let count_DocTypes = {};

        for await (const line of rl)
        {
            let lineObj = JSON.parse(line);

            lineObj.stations = {};

            if(! count_DocTypes[lineObj.docType])
            {
                count_DocTypes[lineObj.docType] = 0;
            }
            else
            {
                ++count_DocTypes[lineObj.docType];
            }

            if(lineObj.docType == "CTC")
            {
                continue;
            }

            if (lineObj.data)
            {
                for (let i = 0; i < lineObj.data.length; i++)
                {
                    lineObj.stations[lineObj.data[i].name] = lineObj.data[i];
                }
            }
            // lineObj["idx0"] = lineObj.type + ":" + lineObj.subset + ":" + lineObj.version + ":" + lineObj.model;
            lineObj.data = undefined;
            
            if(lineObj.subset == "metar")
            {
                ++count_metar;
                lineObj.subset = "METAR";
                // App.log(LogLevel.INFO, "metar => METAR");
                // let objStr = JSON.stringify(lineObj, undefined, 2);
                // App.log(LogLevel.INFO, objStr);
            }
            // App.log(LogLevel.INFO, objStr);
            await this.collection_METAR.upsert(lineObj.id, lineObj);
            if (((++count_METAR) % 100) == 0)
            {
                App.log(LogLevel.INFO, "METAR:" + count_METAR + "\tmetar => METAR:" + count_metar + "\ttotal:" + (count_METAR));
            }
            if(maxCount > 0 && count_METAR >= maxCount)
            {
                break;
            }
            /*
            if (lineObj.docType === "model")
            {
                await this.collection_model.upsert(lineObj.id, lineObj);
                if (((++count_model) % 100) == 0)
                {
                    App.log(LogLevel.INFO, "model:" + count_model + "\ttotal:" + (count_model + count_obs));
                }
            }
            else
            {
                await this.collection_obs.upsert(lineObj.id, lineObj);
                if (((++count_obs) % 100) == 0)
                {
                    App.log(LogLevel.INFO, "obs :" + count_obs + "\ttotal:" + (count_model + count_obs));
                }
            }
            */
        }
        App.log(LogLevel.INFO, "count_DocTypes:" + JSON.stringify(count_DocTypes, undefined, 2));
        let endTime: number = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tin " + (endTime - startTime) + " ms.");
    }

    public async jsonLinesExamine0()
    {
        App.log(LogLevel.INFO, "jsonLinesExamine0()");

        let startTime: number = (new Date()).valueOf();

        let rs = fs.createReadStream(this.jsonFile);

        const rl = readline.createInterface({
            input: rs,
            crlfDelay: Infinity
        });
        // Note: we use the crlfDelay option to recognize all instances of CR LF
        // ('\r\n') in input.txt as a single line break.

        let count_model: number = 0;
        let count_obs: number = 0;
        for await (const line of rl)
        {
            let lineObj = JSON.parse(line);

            lineObj.stations = {};

            if (lineObj.data)
            {
                for (let i = 0; i < lineObj.data.length; i++)
                {
                    lineObj.stations[lineObj.data[i].name] = lineObj.data[i];
                }
            }
            lineObj["idx0"] = lineObj.type + ":" + lineObj.subset + ":" + lineObj.version + ":" + lineObj.model;
            lineObj.data = undefined;
            let objStr = JSON.stringify(lineObj, undefined, 2);
            // App.log(LogLevel.INFO, objStr);
            if (lineObj.docType === "model")
            {
                if (Object.keys(lineObj.stations).length > 0)
                {
                    // App.log(LogLevel.INFO, "model station count:" + Object.keys(lineObj.stations).length);
                }
            }
            else
            {
                if (Object.keys(lineObj.stations).length > 0)
                {
                    App.log(LogLevel.INFO, "model station count:" + Object.keys(lineObj.stations).length);
                    App.log(LogLevel.INFO, "idx0:" + lineObj.idx0);
                    let objStr = JSON.stringify(lineObj, undefined, 2);
                    // App.log(LogLevel.INFO, objStr);
                    App.log(LogLevel.INFO, "fcstValidEpoch:" + lineObj.fcstValidEpoch);
                }
            }
        }
        let endTime: number = (new Date()).valueOf();
        App.log(LogLevel.INFO, "\tin " + (endTime - startTime) + " ms.");
    }
}

export { CbUpload };

