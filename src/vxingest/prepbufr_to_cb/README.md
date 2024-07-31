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

```text
    To begin with, a PREPBUFR file does not always contain, within each single data subset, the data for an entire report! Instead, for reports which contain mass (i.e. temperature, moisture, etc.) as well as wind (i.e. direction and speed, U and V component, etc.) data values, such data values are stored within two separate but adjacent (within the overall file) data subsets, where each related subset, quite obviously, contains the same report time, location, station identification, etc. information as the other, but where the "mass" subset contains the pressures and/or height levels at which "mass" data values occur, while the corresponding "wind" subset contains the levels at which "wind" data values occur. While it is true that this may, in some cases, cause the same pressure and/or height level to appear in both subsets, this separation is nonetheless maintained for historical reasons peculiar to NCEP. At any rate, the below program will actually merge all of the data from both subsets into a single, unified report in such cases, so that the final decoded output is clearer and more intuitive.
```

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
RUC domain
RRFS North American domain
Great Lakes
Global (all lat/lon)
Tropics (-20 <= lat <= +20)
Southern Hemisphere (-80 <= lat < -20)
Northern Hemisphere (+20 < lat <= +80)
Arctic (lat >= +70) -- Might want to change this to lat >= 60N to match EMC?
Antarctic (lat <= -70) -- Might want to change this to lat <= 60S to match EMC?
Alaska
Hawaii
HRRR domain
Eastern HRRR domain
Western HRRR domain
CONUS
Eastern CONUS (lon <= 100W)
Western CONUS (lon <= 100W)
Northeastern CONUS
Southeastern CONUS
Central CONUS
Southern CONUS
Northwest CONUS
Southern Plain
