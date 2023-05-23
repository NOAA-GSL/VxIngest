Configuration is loaded from ./config/config.json
see config.json on how to override the setings.json bucket setting

TODO : need to figure out a private settings file location ...
Make sure there is a linked settings file: ./settings/settings.json

To run all tests, do (at cb_jest_tests folder):
    npm install
    npm test
OR
To run specific test suites:
    npm test basic
    npm test MATS_queries
    npm test experimental

To run a particulat test, do:
    npm test -- -t="Establish CouchBase connection"
    npm test -- -t="Get METAR count"

Observed run times:
    final_TimeSeries.sql: 7.3 s
    final_Map.sql: 5.5s
    final_DieOff.sql: 165s
    final_ValidTime.sql: 4.7s
    final_DailyModelCycle.sql: 7.6s
    
