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
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.CbUpload = void 0;
const fs_1 = __importDefault(require("fs"));
const readline = require("readline");
const App_1 = require("./App");
const couchbase_1 = require("couchbase");
class CbUpload {
    constructor() {
        // public jsonFile = '/scratch/mdatatest/xaa';
        // public jsonFile = '/Users/gopa.padmanabhan/scratch/mdatatest/mdatatest_export_gopa.json';
        this.jsonFile = '/Users/gopa.padmanabhan/scratch/mdatatest/xaa';
        // public clusterConnStr: string = 'couchbase://localhost'
        this.clusterConnStr = 'adb-cb1.gsd.esrl.noaa.gov';
        this.username = 'avid';
        this.password = 'pwd_av!d';
        this.bucketName = 'mdatatest';
        this.cluster = null;
        this.bucket = null;
        this.collection_default = null;
        this.collection_obs = null;
        this.collection_model = null;
        this.collection_METAR = null;
    }
    init() {
        return __awaiter(this, void 0, void 0, function* () {
            this.cluster = yield (0, couchbase_1.connect)(this.clusterConnStr, {
                username: this.username,
                password: this.password,
                timeouts: {
                    kvTimeout: 10000, // milliseconds
                },
            });
            this.bucket = this.cluster.bucket(this.bucketName);
            // Get a reference to the default collection, required only for older Couchbase server versions
            this.collection_default = this.bucket.defaultCollection();
            this.collection_obs = this.bucket.scope('_default').collection('obs');
            this.collection_model = this.bucket.scope('_default').collection('model');
            this.collection_METAR = this.bucket.scope('_default').collection('METAR');
            App_1.App.log(App_1.LogLevel.INFO, "Connected!");
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
        });
    }
    shutdown() {
        return __awaiter(this, void 0, void 0, function* () {
        });
    }
    readJsonAll() {
        return __awaiter(this, void 0, void 0, function* () {
            var settings = JSON.parse(fs_1.default.readFileSync(this.jsonFile, 'utf-8'));
            console.log(settings);
        });
    }
    readJsonLines() {
        var e_1, _a;
        return __awaiter(this, void 0, void 0, function* () {
            console.log("readJsonLines()");
            let rs = fs_1.default.createReadStream(this.jsonFile);
            const rl = readline.createInterface({
                input: rs,
                crlfDelay: Infinity
            });
            try {
                // Note: we use the crlfDelay option to recognize all instances of CR LF
                // ('\r\n') in input.txt as a single line break.
                for (var rl_1 = __asyncValues(rl), rl_1_1; rl_1_1 = yield rl_1.next(), !rl_1_1.done;) {
                    const line = rl_1_1.value;
                    let lineObj = JSON.parse(line);
                    let objStr = JSON.stringify(lineObj, undefined, 2);
                    console.log(objStr);
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (rl_1_1 && !rl_1_1.done && (_a = rl_1.return)) yield _a.call(rl_1);
                }
                finally { if (e_1) throw e_1.error; }
            }
        });
    }
    uploadJsonLinesDefault() {
        var e_2, _a;
        return __awaiter(this, void 0, void 0, function* () {
            App_1.App.log(App_1.LogLevel.INFO, "uploadJsonLinesDefault()");
            let startTime = (new Date()).valueOf();
            let rs = fs_1.default.createReadStream(this.jsonFile);
            const rl = readline.createInterface({
                input: rs,
                crlfDelay: Infinity
            });
            // Note: we use the crlfDelay option to recognize all instances of CR LF
            // ('\r\n') in input.txt as a single line break.
            let count = 0;
            try {
                for (var rl_2 = __asyncValues(rl), rl_2_1; rl_2_1 = yield rl_2.next(), !rl_2_1.done;) {
                    const line = rl_2_1.value;
                    let lineObj = JSON.parse(line);
                    lineObj.stations = {};
                    if (lineObj.data) {
                        for (let i = 0; i < lineObj.data.length; i++) {
                            lineObj.stations[lineObj.data[i].name] = lineObj.data[i];
                        }
                    }
                    lineObj["idx0"] = lineObj.type + ":" + lineObj.subset + ":" + lineObj.version + ":" + lineObj.model;
                    lineObj.data = undefined;
                    let objStr = JSON.stringify(lineObj, undefined, 2);
                    // App.log(LogLevel.INFO, objStr);
                    yield this.collection_default.upsert(lineObj.id, lineObj);
                    if (((++count) % 100) == 0) {
                        console.log(count);
                    }
                }
            }
            catch (e_2_1) { e_2 = { error: e_2_1 }; }
            finally {
                try {
                    if (rl_2_1 && !rl_2_1.done && (_a = rl_2.return)) yield _a.call(rl_2);
                }
                finally { if (e_2) throw e_2.error; }
            }
            let endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tin " + (endTime - startTime) + " ms.");
        });
    }
    uploadJsonLines() {
        var e_3, _a;
        return __awaiter(this, void 0, void 0, function* () {
            App_1.App.log(App_1.LogLevel.INFO, "uploadJsonLines()");
            let startTime = (new Date()).valueOf();
            let rs = fs_1.default.createReadStream(this.jsonFile);
            const rl = readline.createInterface({
                input: rs,
                crlfDelay: Infinity
            });
            // Note: we use the crlfDelay option to recognize all instances of CR LF
            // ('\r\n') in input.txt as a single line break.
            let count_model = 0;
            let count_obs = 0;
            let count_METAR = 0;
            try {
                for (var rl_3 = __asyncValues(rl), rl_3_1; rl_3_1 = yield rl_3.next(), !rl_3_1.done;) {
                    const line = rl_3_1.value;
                    let lineObj = JSON.parse(line);
                    lineObj.stations = {};
                    if (lineObj.data) {
                        for (let i = 0; i < lineObj.data.length; i++) {
                            lineObj.stations[lineObj.data[i].name] = lineObj.data[i];
                        }
                    }
                    lineObj["idx0"] = lineObj.type + ":" + lineObj.subset + ":" + lineObj.version + ":" + lineObj.model;
                    lineObj.data = undefined;
                    let objStr = JSON.stringify(lineObj, undefined, 2);
                    // App.log(LogLevel.INFO, objStr);
                    yield this.collection_METAR.upsert(lineObj.id, lineObj);
                    if (((++count_METAR) % 100) == 0) {
                        App_1.App.log(App_1.LogLevel.INFO, "METAR:" + count_METAR + "\ttotal:" + (count_METAR));
                    }
                }
            }
            catch (e_3_1) { e_3 = { error: e_3_1 }; }
            finally {
                try {
                    if (rl_3_1 && !rl_3_1.done && (_a = rl_3.return)) yield _a.call(rl_3);
                }
                finally { if (e_3) throw e_3.error; }
            }
            let endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tin " + (endTime - startTime) + " ms.");
        });
    }
    jsonLinesExamine0() {
        var e_4, _a;
        return __awaiter(this, void 0, void 0, function* () {
            App_1.App.log(App_1.LogLevel.INFO, "jsonLinesExamine0()");
            let startTime = (new Date()).valueOf();
            let rs = fs_1.default.createReadStream(this.jsonFile);
            const rl = readline.createInterface({
                input: rs,
                crlfDelay: Infinity
            });
            // Note: we use the crlfDelay option to recognize all instances of CR LF
            // ('\r\n') in input.txt as a single line break.
            let count_model = 0;
            let count_obs = 0;
            try {
                for (var rl_4 = __asyncValues(rl), rl_4_1; rl_4_1 = yield rl_4.next(), !rl_4_1.done;) {
                    const line = rl_4_1.value;
                    let lineObj = JSON.parse(line);
                    lineObj.stations = {};
                    if (lineObj.data) {
                        for (let i = 0; i < lineObj.data.length; i++) {
                            lineObj.stations[lineObj.data[i].name] = lineObj.data[i];
                        }
                    }
                    lineObj["idx0"] = lineObj.type + ":" + lineObj.subset + ":" + lineObj.version + ":" + lineObj.model;
                    lineObj.data = undefined;
                    let objStr = JSON.stringify(lineObj, undefined, 2);
                    // App.log(LogLevel.INFO, objStr);
                    if (lineObj.docType === "model") {
                        if (Object.keys(lineObj.stations).length > 0) {
                            // App.log(LogLevel.INFO, "model station count:" + Object.keys(lineObj.stations).length);
                        }
                    }
                    else {
                        if (Object.keys(lineObj.stations).length > 0) {
                            App_1.App.log(App_1.LogLevel.INFO, "model station count:" + Object.keys(lineObj.stations).length);
                            App_1.App.log(App_1.LogLevel.INFO, "idx0:" + lineObj.idx0);
                            let objStr = JSON.stringify(lineObj, undefined, 2);
                            // App.log(LogLevel.INFO, objStr);
                            App_1.App.log(App_1.LogLevel.INFO, "fcstValidEpoch:" + lineObj.fcstValidEpoch);
                        }
                    }
                }
            }
            catch (e_4_1) { e_4 = { error: e_4_1 }; }
            finally {
                try {
                    if (rl_4_1 && !rl_4_1.done && (_a = rl_4.return)) yield _a.call(rl_4);
                }
                finally { if (e_4) throw e_4.error; }
            }
            let endTime = (new Date()).valueOf();
            App_1.App.log(App_1.LogLevel.INFO, "\tin " + (endTime - startTime) + " ms.");
        });
    }
}
exports.CbUpload = CbUpload;
