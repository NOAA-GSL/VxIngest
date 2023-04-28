'use strict';
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const CbRunQueries_1 = require("./CbRunQueries");
const CbQueriesTimeSeriesStations_1 = require("./CbQueriesTimeSeriesStations");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        let cbq = new CbRunQueries_1.CbRunQueries();
        let cbqTimeSeries = new CbQueriesTimeSeriesStations_1.CbQueriesTimeSeriesStations();
        yield cbq.init();
        yield cbqTimeSeries.init();
        // await cbqTimeSeries.processStationQuery("./dataFiles/station_names_3.json", "HRRR_OPS", 6, 3000, true);
        // await cbqTimeSeries.processStationQuery("./dataFiles/station_names_N.json", "HRRR_OPS", 6, 3000, true);
        yield cbqTimeSeries.processStationQuery("./dataFiles/station_names_all.json", "HRRR_OPS", 6, 3000, true);
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
    });
}
main()
    .catch((err) => {
    console.log('ERR:', err);
    process.exit(1);
})
    .then(() => process.exit(0));
