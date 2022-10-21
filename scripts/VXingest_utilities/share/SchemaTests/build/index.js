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
const CbUpload_1 = require("./CbUpload");
const CbTimeSeries0_1 = require("./CbTimeSeries0");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        let cbu = new CbUpload_1.CbUpload();
        yield cbu.init();
        // await cbu.uploadJsonLinesDefault();
        yield cbu.uploadJsonLines();
        // await cbu.jsonLinesExamine0();
        let ts0 = new CbTimeSeries0_1.CbTimeSeries0();
        // await ts0.doTimeSeriesQuery0(cbu.bucket, './queries/timeseries_mdata_3.sql');
        // await ts0.doTimeSeriesQueryA(cbu.bucket, './queries/timeseries_mdata_3_model.sql', './queries/timeseries_mdata_3_obs.sql');
    });
}
main()
    .catch((err) => {
    console.log('ERR:', err);
    process.exit(1);
})
    .then(() => process.exit(0));
