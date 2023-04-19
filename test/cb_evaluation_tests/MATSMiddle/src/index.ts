'use strict'

import { CbQueriesTimeSeriesStations } from "./CbQueriesTimeSeriesStations";

async function main()
{
    let cbq : CbQueriesTimeSeriesStations = new CbQueriesTimeSeriesStations();
    await cbq.init();
    
    // await cbq.runOrgStationQueryFinalSaveToFile();
    await cbq.processStationQuery_N_Stations();
}

main()
    .catch((err) =>
    {
        console.log('ERR:', err)
        process.exit(1)
    })
    .then(() => process.exit(0))
