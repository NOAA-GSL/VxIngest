# prepbufr ingest to couchbase

## purpose

These programs are intended to import prepbufr data into Couchbase taking advantage of the GSL Couchbase data schema
that has been developed by the GSL AVID model verification team.

## Approach

These programs use a JOB document to define which ingest templates are to be used, a credentials file to provide database authentication, command line parameters for run time options, and the associated ingest template documents from the database that are specified in the JOB document.

The ingest_document_ids: ['MD:V01:RAOB:obs:ingest:prepbufr'] line defines
a list of metadata documents (might be just one). These documents define how the program will operate.
The 'MD:V01:RAOB:obs:ingest:prepbufr' value is the id of a couchbase metadata document.
This document MUST exist on the couchbase cluster defined by cb_host in an associated credentials file (the name of which is provided as a command line parameter) and MUST be readable by the cb_user.

The prepbufr ingest documents have an additional section "mnemonic_mapping" that serves the purpose of mapping prepbufr mnemonics to the variables used in
the template DSL.

This issue demonstrates ways to make a query of a map of maps
See [issue](https://www.couchbase.com/forums/t/querying-a-map-of-maps-with-a-dynamic-key/30019)
For example:

```SQL
WITH ks AS (["70026",   "70026"])
SELECT ARRAY r.data.[v] FOR v IN ks END AS station
FROM vxdata._default.RAOB AS r USE KEYS "DD:V01:RAOB:obs:prepbufr:170:1717567200";
```

## stations

The station documents in the RAOB collection are RAOB stations. They all have ids like "MD:V01:RAOB:station:1001".

example

```json
{
  "id": "MD:V01:RAOB:station:1001",
  "wmoid": 1001,
  "name": "ENJA",
  "geo": [
    {
      "lat": 70.93,
      "lon": -8.67,
      "elev": 9,
      "firstTime": 0,
      "lastTime": 1714068000
    }
  ],
  "subset": "RAOB",
  "type": "MD",
  "docType": "station",
  "version": "V01",
  "description": "JAN MAYEN(NOR-NAVY)",
  "near_airport": "",
  "gps_date": "",
  "updateTime": "2024-04-25 12:00:00"
}
  ```

## station geo list

The geo list is based on the lowest height (highest pressure) lat / lon values which should most closely represent the lat / lon of the station. The handle_station
handler method will create a new station if it can find the station data in the station reference document "MD:V01:RAOB:stationReference", which contains the latest set of stations from raob.com. If it cannot find the station the data for this "unknown" station will be thrown away. If the highest pressure lat / lon values are significantly different from the latest ones in the geo list, a new geo list entry will be added to the station under the assumption that the station launch site has moved.

## data extraction from the prepbufr files

We need to take MASS values from the MASS report (120) and the WIND values from the wind report (220) and merge them into our data for the particular site and launch.

This website describes the report types.
[report types](https://www.nco.ncep.noaa.gov/sib/decoders/BUFRLIB/toc/prepbufr/)

We are assuming that the wind data are always in report 220 (which is WIND Report - Rawinsonde ) and the other variables are in report 120 - the MASS report. It's a little vague when it says "does not always contain" but if it doesn't contain both we have to mask the part that isn't present or through the whole station data away.

> To begin with, a PREPBUFR file does not always contain, within each single data subset, the data for an entire report! Instead, for reports which contain mass (i.e. temperature, moisture, etc.) as well as wind (i.e. direction and speed, U and V component, etc.) data values, such data values are stored within two separate but adjacent (within the overall file) data subsets, where each related subset, quite obviously, contains the same report time, location, station identification, etc. information as the other, but where the "mass" subset contains the pressures and/or height levels at which "mass" data values occur, while the corresponding "wind" subset contains the levels at which "wind" data values occur. While it is true that this may, in some cases, cause the same pressure and/or height level to appear in both subsets, this separation is nonetheless maintained for historical reasons peculiar to NCEP.

The report types are described here [report_types](https://www.emc.ncep.noaa.gov/emc/pages/infrastructure/bufrlib/tables/CodeFlag_0_STDv41_LOC7.html#055007)

```text
120 MASS Report - Rawinsonde
. . .
220 WIND Report - Rawinsonde
```

We assume that we always have to take MASS values from the MASS report (120) and the WIND values from the wind report (220).
We also assume that if there is no wind report wind data should just be masked in our data.

## Builder class

The builder is [PrepbufrBuilder](https://github.com/NOAA-GSL/VxIngest/tree/main/src/vxingest)

There is a base PrepbufrBuilder which has the generic code for reading a prepbufr file and a specialized PrepbufrRaobsObsBuilderV01 class which knows how to build RAOBS from a prepbufr file.

## Region list

I'm only putting this here temporarily so that I don't lose it before it gets implemented.

* RUC domain
* RRFS North American domain
* Great Lakes
* Global (all lat/lon)
* Tropics (-20 <= lat <= +20)
* Southern Hemisphere (-80 <= lat < -20)
* Northern Hemisphere (+20 < lat <= +80)
* Arctic (lat >= +70) -- Might want to change this to lat >= 60N to match EMC?
* Antarctic (lat <= -70) -- Might want to change this to lat <= 60S to match EMC?
* Alaska
* Hawaii
* HRRR domain
* Eastern HRRR domain
* Western HRRR domain
* CONUS
* Eastern CONUS (lon <= 100W)
* Western CONUS (lon <= 100W)
* Northeastern CONUS
* Southeastern CONUS
* Central CONUS
* Southern CONUS
* Northwest CONUS
* Southern Plain

## Ingest template
The ingest template for prepbufr RAOBS is "MD:V01:RAOB:obs:ingest:prepbufr".
It follows the same small Domain Specific Language (DSL) that all ingest templates follow. This is the template portion...

```json
"template": {
    "data": {
      "*stationName": {
        "temperature": "*temperature",
        "dewpoint": "*dewpoint",
        "relative_humidity": "*relative_humidity",
        "specific_humidity": "*specific_humidity",
        "pressure": "*pressure",
        "height": "*height",
        "wind_speed": "&knots_to_meters_per_second|*wind_speed",
        "U-Wind": "*U-Wind",
        "V-Wind": "*V-Wind",
        "wind_direction": "*wind_direction",
        "stationName": "&handle_station|*stationName"
      }
    },
        "units": {
      "temperature": "deg F",
      "dewpoint": "deg F",
      "relative_humidity": "percent",
      "specific_humidity": "m/kg",
      "pressure": "mb",
      "height": "meters",
      "wind_speed": "mph",
      "U-Wind": "mph",
      "V-Wind": "mph",
      "wind_direction": "degrees"
    },
    "level": "*level",
    "dataSourceId": "GDAS",
    "docType": "obs",
    "subDocType": "prepbufr",
    "fcstValidISO": "&get_valid_time_iso",
    "fcstValidEpoch": "&get_valid_time_epoch",
    "id": "DD:V01:RAOB:obs:prepbufr:*level:&get_valid_time_epoch",
    "subset": "RAOB",
    "type": "DD",
    "version": "V01"
  },
  ```

The variable names will not be found in any prepbufr file. They are the kind of variable names that we use in our Couchbase schema.
To map those names into the prepbufr data there is another section in the ingest document - "mnemonic_mapping".
For each element there is a mnemonic, and an intent. The mnemonic identifies the actual bufr mnemonic to be found in the prepbufr file.
The intent indicates what data type the variable is to be translated into - str, float, or int. The events flag indicates whether
the builder should consider event program types when decoding the field. If the events flag is True there must also be
an "event_program_code_mnemonic" , and an "event_value". The event_program_code_mnemonic indicates what mnemonic the program code will be found under.
The value indicates the desired value. For example for temperature...

```json
"temperature": {
        "mnemonic": "TOB",                    the mnemonic for temperature
        "event_program_code_mnemonic": "TPC", the associated event program code for temperature
        "intent": "float",                    temperature should be decoded to a float
        "event_value": 1                      the event program code that is desired is 1 (which is initial temperature, 8 would be virtual temp)
      },
```

This way any of the voluminous data that is contained in a prepbufr RAOB file can be succinctly decoded.
There are four sections of mappings.

1. `header`        basic header data like lat, lon, and station name
2. `q_marker`      quality data
3. `obs_err`       observation error data
4. `obs_data_120`  observation MASS data
5. `obs_data_220`  observation WIND data

```json
  "mnemonic_mapping": {
    "bufr_msg_type": "ADPUPA",   This is the subset type of interest - in this case radiosonde data
    "bufr_report_types": [
      120,                       This is the MASS report identifier. The MASS report contains variable data like temperature and dewpoint
      220                        This is the WIND report
    ],
    "header": {                  This is the header section
      "events": false,
      "station_id": {
        "mnemonic": "SID",       Station name
        "intent": "str"
      },
      "lon": {
        "mnemonic": "XOB",
        "intent": "float"
      },
      "lat": {
        "mnemonic": "YOB",
        "intent": "float"
      },
      "obs-cycle_time": {
        "mnemonic": "DHR",
        "intent": "float"
      },
      "elevation": {
        "mnemonic": "ELV",
        "intent": "float"
      },
      "data_dump_report_type": {
        "TYP": 220,
        "mnemonic": "T29",
        "intent": "int"
      },
      "report_type": {
        "mnemonic": "TYP",
        "intent": "int"
      }
    },
    "q_marker": {
      "events": false,
      "pressure_q_marker": {
        "mnemonic": "PQM",
        "intent": "int"
      },
      "specific_humidity_q_marker": {
        "mnemonic": "QQM",
        "intent": "int"
      },
      "temperature_q_marker": {
        "mnemonic": "TQM",
        "intent": "int"
      },
      "height_q_marker": {
        "mnemonic": "ZQM",
        "intent": "int"
      },
      "u_v_wind_q_marker": {
        "mnemonic": "WQM",
        "intent": "int"
      },
      "wind_direction_q_marker": {
        "mnemonic": "DFP",
        "intent": "int"
      },
      "u_v_component_wind_q_marker": {
        "mnemonic": "WPC",
        "intent": "int"
      }
    },
    "obs_err": {
      "events": false,
      "pressure_obs_err": {
        "mnemonic": "POE",
        "intent": "float"
      },
      "height_obs_err": {
        "mnemonic": "ZOE",
        "intent": "float"
      },
      "relative_humidity_obs_err": {
        "mnemonic": "QOE",
        "intent": "float"
      },
      "temperature_obs_err": {
        "mnemonic": "TOE",
        "intent": "float"
      },
      "winds_obs_err": {
        "mnemonic": "WOE",
        "intent": "float"
      }
    },
    "obs_data_120": {
      "events": true,
      "temperature": {
        "mnemonic": "TOB",
        "event_program_code_mnemonic": "TPC",
        "intent": "float",
        "event_value": 1
      },
      "dewpoint": {
        "mnemonic": "TDO",
        "intent": "float",
        "event_value": 1
      },
      "relative_humidity": {
        "mnemonic": "RHO",
        "intent": "float"
      },
      "specific_humidity": {
        "mnemonic": "QOB",
        "event_program_code_mnemonic": "QPC",
        "intent": "float",
        "event_value": 1
      },
      "pressure": {
        "mnemonic": "POB",
        "event_program_code_mnemonic": "PPC",
        "intent": "float",
        "event_value": 1
      },
      "height": {
        "mnemonic": "ZOB",
        "event_program_code_mnemonic": "ZPC",
        "intent": "float",
        "event_value": 1
      }
    },
    "obs_data_220": {
      "events": true,
      "pressure": {
        "mnemonic": "POB",
        "event_program_code_mnemonic": "PPC",
        "intent": "float",
        "event_value": 1
      },
      "wind_speed": {
        "mnemonic": "FFO",
        "intent": "float"
      },
      "U-Wind": {
        "mnemonic": "UOB",
        "event_program_code_mnemonic": "WPC",
        "intent": "float",
        "event_value": 1
      },
      "V-Wind": {
        "mnemonic": "VOB",
        "event_program_code_mnemonic": "WPC",
        "intent": "float",
        "event_value": 1
      },
      "wind_direction": {
        "mnemonic": "DDO",
        "event_program_code_mnemonic": "DFP",
        "intent": "float",
        "event_value": 1
      }
    }
  }
  ```