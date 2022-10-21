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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.CbTimeSeries0 = void 0;
const fs_1 = __importDefault(require("fs"));
const App_1 = require("./App");
class CbTimeSeries0 {
    doTimeSeriesQuery0(bucket, query_file) {
        return __awaiter(this, void 0, void 0, function* () {
            App_1.App.elapsed_time("CbTimeSeries0 start.");
            const data = fs_1.default.readFileSync(query_file, 'utf8');
            console.log(data);
            const scope = yield bucket.scope('_default');
            const queryResult = yield scope.query(data, {
                parameters: ['United States'],
            });
            console.log('Query Results:');
            queryResult.rows.forEach((row) => {
                console.log(row);
            });
            App_1.App.elapsed_time("CbTimeSeries0 end.");
        });
    }
    doTimeSeriesQueryA(bucket, query_file_model, query_file_obs) {
        return __awaiter(this, void 0, void 0, function* () {
            App_1.App.elapsed_time("CbTimeSeries0 start.");
            const sql_model = fs_1.default.readFileSync(query_file_model, 'utf8');
            console.log(sql_model);
            const sql_obs = fs_1.default.readFileSync(query_file_obs, 'utf8');
            console.log(sql_obs);
            const scope = yield bucket.scope('_default');
            const result_model = yield scope.query(sql_model, {
                parameters: ['United States'],
            });
            const result_obs = yield scope.query(sql_obs, {
                parameters: ['United States'],
            });
            console.log('Query Results:');
            result_obs.rows.forEach((row) => {
                if (row.stations && Object.keys(row.stations).length > 0) {
                    console.log(row);
                }
            });
            App_1.App.elapsed_time("CbTimeSeries0 end.");
        });
    }
}
exports.CbTimeSeries0 = CbTimeSeries0;
