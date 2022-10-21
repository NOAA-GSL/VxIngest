'use strict'

import { CbUpload } from "./CbUpload";
import { CbTimeSeries0 } from "./CbTimeSeries0";

async function main()
{
    let cbu : CbUpload = new CbUpload();
    await cbu.init();
    // await cbu.uploadJsonLinesDefault();
    await cbu.uploadJsonLines();
    // await cbu.jsonLinesExamine0();
    
    let ts0 = new CbTimeSeries0();
    // await ts0.doTimeSeriesQuery0(cbu.bucket, './queries/timeseries_mdata_3.sql');
    // await ts0.doTimeSeriesQueryA(cbu.bucket, './queries/timeseries_mdata_3_model.sql', './queries/timeseries_mdata_3_obs.sql');
}

main()
    .catch((err) =>
    {
        console.log('ERR:', err)
        process.exit(1)
    })
    .then(() => process.exit(0))
