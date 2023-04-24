'use strict'

import { CbRunQueries } from "./CbRunQueries";
import { CbQueriesTimeSeriesStations } from "./CbQueriesTimeSeriesStations";

async function main()
{
    let cbq : CbRunQueries = new CbRunQueries();
    let cbqTimeSeries : CbQueriesTimeSeriesStations = new CbQueriesTimeSeriesStations();
    await cbq.init();
    await cbqTimeSeries.init();
    
    await cbqTimeSeries.processStationQuery();
    
    
    //await cbq.runQueryFile("get_N_stations_mfve_obs.sql", true);

    // await cbq.runOrgStationQueryFinalSaveToFile();
    // await cbq.runObsModelQueries("get_distinct_fcstValidEpoch_obs.sql", "get_distinct_fcstValidEpoch_model.sql", true);
    // await cbq.runObsModelQueries("get_N_stations_obs.sql", "get_N_stations_model.sql", true);
    // await cbq.runObsModelQueries("get_N_stations_obs_group_order.sql", "get_N_stations_model.sql", true);
    // await cbq.runObsModelQueries("get_all_stations_obs.sql", "get_all_stations_model.sql", true);
    // await cbq.runQueryFile("get_stations_obs_0.sql", true);
    // await cbq.runQueryFile("get_all_stations_obs.sql", true);
    // await cbq.runQueryFile("get_N_stations_obs.sql", true);
    // await cbq.runQueryFile("get_N_stations_obs_Ceiling.sql", true);
}

main()
    .catch((err) =>
    {
        console.log('ERR:', err)
        process.exit(1)
    })
    .then(() => process.exit(0))
