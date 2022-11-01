const fs = require('fs');
var couchbase = require('couchbase');

const bucketName = 'mdatatest';
const clusterConnStr = 'adb-cb1.gsd.esrl.noaa.gov';
const settingsFile = '/Users/gopa.padmanabhan/mats-settings/configurations/dev/settings/cb-ceiling/settings.json';

jest.setTimeout(20000);

describe("Connection function", () =>
{
    test("Tests connnection to CouchBase server", async () =>
    {
        let cluster = await connectToCb();
        expect(cluster != undefined);

        let bucket = cluster.bucket(bucketName);
        expect(bucket != undefined);

        let collection_METAR = bucket.scope('_default').collection('METAR');
        let res = await run_METAR_count(bucket);
        expect(res != undefined);

        res = await run_q0(bucket);
        expect(res != undefined);
    });
});

async function connectToCb()
{
    console.log("connectToCb()");

    let startTime = (new Date()).valueOf();
    
    var settings = JSON.parse(fs.readFileSync(settingsFile, 'utf-8'));

    let cluster = await couchbase.connect(clusterConnStr, {
        username: settings.private.databases[0].user,
        password: settings.private.databases[0].password,
        timeouts: {
            kvTimeout: 10000, // milliseconds
        },
    });

    let endTime = (new Date()).valueOf();

    console.log("\tconnectToCb() in " + (endTime - startTime) + " ms.");

    return cluster;
}

async function run_METAR_count(bckt)
{
    console.log("run_q0(bckt)");

    let startTime = (new Date()).valueOf();

    const clusterConnStr = 'adb-cb1.gsd.esrl.noaa.gov';
    const queryFile = '/Users/gopa.padmanabhan/VxIngest/scripts/VXingest_utilities/share/cbQueryTests/test_queries/METAR_count.sql';
    const qstr = fs.readFileSync(queryFile, 'utf-8');

    const queryResult = await bckt.scope('_default')
        .query(qstr, {
            parameters: [],
        });
    queryResult.rows.forEach((row) =>
    {
        console.log(row)
    });

    let endTime = (new Date()).valueOf();
    console.log("\trun_METAR_count() in " + (endTime - startTime) + " ms.");

    return queryResult;
}

async function run_q0(bckt)
{
    console.log("run_q0(bckt)");

    const clusterConnStr = 'adb-cb1.gsd.esrl.noaa.gov';
    const queryFile = '/Users/gopa.padmanabhan/VxIngest/scripts/VXingest_utilities/share/cbQueryTests/test_queries/q0.sql';
    const qstr = fs.readFileSync(queryFile, 'utf-8');

    let startTime = (new Date()).valueOf();
    const queryResult = await bckt.scope('_default')
        .query(qstr, {
            parameters: [],
        });

    /*
    queryResult.rows.forEach((row) =>
    {
        console.log(row)
    });
    */

    let endTime = (new Date()).valueOf();
    console.log("\trun_q0() in " + (endTime - startTime) + " ms.");
    return queryResult;
}

