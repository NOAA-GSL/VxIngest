'use strict'

import { CbUpload } from "./CbUpload";
import { CbTimeSeries0 } from "./CbTimeSeries0";

async function main()
{
    let cbu: CbUpload = new CbUpload();
    // await cbu.init('/Users/gopa.padmanabhan/mats-settings/configurations/dev/settings/cb-ceiling/settings.json', 'vxdata');
    await cbu.init('/home/gopa/mats-settings/configurations/dev/settings/cb-ceiling/settings.json', 'vxdata');
    // await cbu.uploadJsonLinesDefault();
    // await cbu.uploadJsonLines('/Users/gopa.padmanabhan/scratch/mdatatest/mdatatest_export_gopa.json', 1000);
    await cbu.uploadJsonLines('/scratch/mdata_export.json', 0);
    // await cbu.uploadJsonLines('/scratch/mdatatest/xaa', 200);
    // await cbu.uploadJsonLines('/scratch/mdatatest/mdatatest_export_gopa.json', 10);
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
